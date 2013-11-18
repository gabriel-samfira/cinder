# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2012 NetApp, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import errno
import os

from oslo.config import cfg

from cinder.brick.remotefs import remotefs
from cinder import exception
from cinder.openstack.common import log as logging
from cinder.openstack.common import processutils as putils
from cinder import units
from cinder import utils
from cinder.volume.drivers import nfs


VERSION = '1.1.0'

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.StrOpt('smbfs_shares_config',
               default='/etc/cinder/smbfs_shares',
               help='File with the list of available smbfs shares'),
    cfg.BoolOpt('smbfs_sparsed_volumes',
                default=True,
                help=('Create volumes as sparsed files which take no space.'
                      'If set to False volume is created as regular file.'
                      'In such case volume creation takes a lot of time.')),
    cfg.FloatOpt('smbfs_used_ratio',
                 default=0.95,
                 help=('Percent of ACTUAL usage of the underlying volume '
                       'before no new volumes can be allocated to the volume '
                       'destination.')),
    cfg.FloatOpt('smbfs_oversub_ratio',
                 default=1.0,
                 help=('This will compare the allocated to available space on '
                       'the volume destination.  If the ratio exceeds this '
                       'number, the destination will no longer be valid.')),
    cfg.StrOpt('smbfs_mount_point_base',
               default='$state_path/mnt',
               help=('Base dir containing mount points for smbfs shares.')),
    cfg.BoolOpt('smbfs_qcow2_volumes',
                default=False,
                help=('Create volumes as QCOW2 files rather than raw files.')),
    cfg.StrOpt('smbfs_mount_options',
               default=None,
               help=('Mount options passed to the smbfs client. See section '
                     'of the smbfs man page for details.')),
]


CONF = cfg.CONF
CONF.register_opts(volume_opts)


class SmbfsDriver(nfs.RemoteFsDriver):
    """SMBFS based cinder driver. Creates file on SMBFS share for using it
    as block device on hypervisor.
    """

    driver_volume_type = 'smbfs'
    driver_prefix = 'smbfs'
    volume_backend_name = 'Generic_SMBFS'
    VERSION = VERSION

    def __init__(self, execute=putils.execute, *args, **kwargs):
        self._remotefsclient = None
        super(SmbfsDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(volume_opts)
        root_helper = utils.get_root_helper()
        base = getattr(self.configuration,
                       'smbfs_mount_point_base',
                       CONF.smbfs_mount_point_base)
        opts = getattr(self.configuration,
                       'smbfs_mount_options',
                       CONF.smbfs_mount_options)
        self._remotefsclient = remotefs.RemoteFsClient(
            'cifs', root_helper, execute=execute,
            smbfs_mount_point_base=base,
            smbfs_mount_options=opts)
        self.img_suffix = None

    def set_execute(self, execute):
        super(SmbfsDriver, self).set_execute(execute)
        if self._remotefsclient:
            self._remotefsclient.set_execute(execute)

    def do_setup(self, context):
        """Any initialization the volume driver does while starting"""
        super(SmbfsDriver, self).do_setup(context)

        config = self.configuration.smbfs_shares_config
        if not config:
            msg = (_("There's no SMBFS config file configured (%s)") %
                   'smbfs_shares_config')
            LOG.warn(msg)
            raise exception.SmbfsException(msg)
        if not os.path.exists(config):
            msg = (_("SMBFS config file at %(config)s doesn't exist") %
                   {'config': config})
            LOG.warn(msg)
            raise exception.SmbfsException(msg)
        if not self.configuration.smbfs_oversub_ratio > 0:
            msg = _(
                "SMBFS config 'smbfs_oversub_ratio' invalid.  Must be > 0: "
                "%s") % self.configuration.smbfs_oversub_ratio

            LOG.error(msg)
            raise exception.SmbfsException(msg)

        if ((not self.configuration.smbfs_used_ratio > 0) and
                (self.configuration.smbfs_used_ratio <= 1)):
            msg = _("SMBFS config 'smbfs_used_ratio' invalid.  Must be > 0 "
                    "and <= 1.0: %s") % self.configuration.smbfs_used_ratio
            LOG.error(msg)
            raise exception.SmbfsException(msg)

        self.shares = {}  # address : options
        self._ensure_shares_mounted()
        # mount.smbfs is not needed. CIFS support is in the kernel

    def delete_volume(self, volume):
        """Deletes a logical volume. Hyper-V cares about the extension...

        :param volume: volume reference
        """
        if not volume['provider_location']:
            LOG.warn(_('Volume %s does not have provider_location specified, '
                     'skipping'), volume['name'])
            return

        self._ensure_share_mounted(volume['provider_location'])
        mounted_path = self.local_path(volume)
        self._execute('rm', '-f', mounted_path, run_as_root=True)

    def _create_vpc_file(self, volume_path, volume_size):
        """Creates a VPC file of a given size."""

        self._execute('qemu-img', 'create', '-f', 'vpc',
                      volume_path, str(volume_size * units.GiB),
                      run_as_root=True)

    def _do_create_volume(self, volume):
        """Create a volume on given smbfs_share.

        :param volume: volume reference
        """

        volume_path = self.local_path(volume)
        volume_size = volume['size']

        LOG.debug(_("creating new volume at %s") % volume_path)

        if os.path.exists(volume_path):
            msg = _('file already exists at %s') % volume_path
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)
        if volume['volume_type']:
            if volume['volume_type']['name'] in ('vpc', 'vhd', 'vhdx'):
                self._create_vpc_file(volume_path, volume_size)
            else:
                raise SmbfsException("Invalid volume type")
        else:
            self.img_suffix = None
            if self.configuration.smbfs_qcow2_volumes:
                self._create_qcow2_file(volume_path, volume_size)
            else:
                if self.configuration.smbfs_sparsed_volumes:
                    self._create_sparsed_file(volume_path, volume_size)
                else:
                    self._create_regular_file(volume_path, volume_size)

        self._set_rw_permissions_for_all(volume_path)

    def _ensure_share_mounted(self, smbfs_share):
        mnt_flags = []
        LOG.debug(">>>>%r" % self.shares)
        LOG.debug(">>>>%r" % smbfs_share)
        if self.shares.get(smbfs_share) is not None:
            mnt_flags = self.shares[smbfs_share].split()
        self._remotefsclient.mount(smbfs_share, mnt_flags)

    def _find_share(self, volume_size_in_gib):
        """Choose SMBFS share among available ones for given volume size.

        For instances with more than one share that meets the criteria, the
        share with the least "allocated" space will be selected.

        :param volume_size_in_gib: int size in GB
        """

        if not self._mounted_shares:
            raise exception.SmbfsNoSharesMounted()

        target_share = None
        target_share_reserved = 0

        for smbfs_share in self._mounted_shares:
            if not self._is_share_eligible(smbfs_share, volume_size_in_gib):
                continue
            total_size, total_available, total_allocated = \
                self._get_capacity_info(smbfs_share)
            if target_share is not None:
                if target_share_reserved > total_allocated:
                    target_share = smbfs_share
                    target_share_reserved = total_allocated
            else:
                target_share = smbfs_share
                target_share_reserved = total_allocated

        if target_share is None:
            raise exception.SmbfsNoSuitableShareFound(
                volume_size=volume_size_in_gib)

        LOG.debug(_('Selected %s as target smbfs share.'), target_share)

        return target_share

    def _is_share_eligible(self, smbfs_share, volume_size_in_gib):
        """Verifies SMBFS share is eligible to host volume with given size.

        First validation step: ratio of actual space (used_space / total_space)
        is less than 'smbfs_used_ratio'. Second validation step: apparent space
        allocated (differs from actual space used when using sparse files)
        and compares the apparent available
        space (total_available * smbfs_oversub_ratio) to ensure enough space is
        available for the new volume.

        :param smbfs_share: smbfs share
        :param volume_size_in_gib: int size in GB
        """

        used_ratio = self.configuration.smbfs_used_ratio
        oversub_ratio = self.configuration.smbfs_oversub_ratio
        requested_volume_size = volume_size_in_gib * units.GiB

        total_size, total_available, total_allocated = \
            self._get_capacity_info(smbfs_share)
        apparent_size = max(0, total_size * oversub_ratio)
        apparent_available = max(0, apparent_size - total_allocated)
        used = (total_size - total_available) / total_size
        if used > used_ratio:
            # NOTE(morganfainberg): We check the used_ratio first since
            # with oversubscription it is possible to not have the actual
            # available space but be within our oversubscription limit
            # therefore allowing this share to still be selected as a valid
            # target.
            LOG.debug(_('%s is above smbfs_used_ratio'), smbfs_share)
            return False
        if apparent_available <= requested_volume_size:
            LOG.debug(_('%s is above smbfs_oversub_ratio'), smbfs_share)
            return False
        if total_allocated / total_size >= oversub_ratio:
            LOG.debug(_('%s reserved space is above smbfs_oversub_ratio'),
                      smbfs_share)
            return False
        return True

    def _get_mount_point_for_share(self, smbfs_share):
        """Needed by parent class."""
        return self._remotefsclient.get_mount_point(smbfs_share)

    def _get_capacity_info(self, smbfs_share):
        """Calculate available space on the SMBFS share.

        :param smbfs_share: example //172.18.194.100/var/smbfs
        """

        mount_point = self._get_mount_point_for_share(smbfs_share)

        df, _ = self._execute('stat', '-f', '-c', '%S %b %a', mount_point,
                              run_as_root=True)
        block_size, blocks_total, blocks_avail = map(float, df.split())
        total_available = block_size * blocks_avail
        total_size = block_size * blocks_total

        du, _ = self._execute('du', '-sb', '--apparent-size', '--exclude',
                              '*snapshot*', mount_point, run_as_root=True)
        total_allocated = float(du.split()[0])
        return total_size, total_available, total_allocated
