///////////////////////////////////////////////////////////////////////////////
///
/// @file Wrapper.cpp
///
/// High-level wrapper around libguestfs C API.
///
/// @author mperevedentsev
///
/// Copyright (c) 2005-2015 Parallels IP Holdings GmbH
///
/// This file is part of Virtuozzo Core. Virtuozzo Core is free
/// software; you can redistribute it and/or modify it under the terms
/// of the GNU General Public License as published by the Free Software
/// Foundation; either version 2 of the License, or (at your option) any
/// later version.
///
/// This program is distributed in the hope that it will be useful,
/// but WITHOUT ANY WARRANTY; without even the implied warranty of
/// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
/// GNU General Public License for more details.
///
/// You should have received a copy of the GNU General Public License
/// along with this program; if not, write to the Free Software
/// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
/// 02110-1301, USA.
///
/// Our contact details: Parallels IP Holdings GmbH, Vordergasse 59, 8200
/// Schaffhausen, Switzerland.
///
///////////////////////////////////////////////////////////////////////////////
#include <boost/make_shared.hpp>

#include "GuestFSWrapper.h"
#include "StringTable.h"

using namespace GuestFS;

namespace
{

const char GUESTFS_DEVICE[] = "/dev/sda";

enum {MAX_BOOTLOADER_SECTS = 4096};
enum {GPT_START_SECTS = 64};
enum {GPT_END_SECTS = 64};
enum {ALIGNMENT_SECTS = 128};

quint64 ceilTo(quint64 bytes, quint64 div)
{
	return (bytes + div - 1) / div * div;
}

quint64 ceilToMb(quint64 bytes)
{
	return ceilTo(bytes, 1024 * 1024);
}

quint64 convertToBytes(const QString &value, const QChar &power, quint64 radixStep = 1024)
{
	quint64 nom, denom = 1;
	if (value.count('.') > 1)
		return 0;
	else if (!value.contains('.'))
		nom = value.toULongLong();
	else
	{
		int index = value.indexOf('.');
		while (++index < value.length())
			denom *= 10;
		QString tmp(value);
		nom = tmp.remove('.').toULongLong();
	}

	char c = power.toUpper().toAscii();
	quint64 multiplier = 1;
	switch (c)
	{
		case 'P':
			multiplier *= radixStep;
		case 'T':
			multiplier *= radixStep;
		case 'G':
			multiplier *= radixStep;
		case 'M':
			multiplier *= radixStep;
		case 'K':
			multiplier *= radixStep;
	};

	return ((nom * multiplier + denom - 1) / denom);
}

} // namespace

namespace Partition
{

////////////////////////////////////////////////////////////
// List

Expected<QString> List::getFirst() const
{
	Expected<QStringList> partitions = get();
	if (!partitions.isOk())
		return partitions;
	return partitions.get().first();
}

Expected<QString> List::getLast() const
{
	Expected<QStringList> partitions = get();
	if (!partitions.isOk())
		return partitions;
	return partitions.get().last();
}

Expected<int> List::getCount() const
{
	Expected<QStringList> partitions = get();
	if (!partitions.isOk())
		return partitions;
	return partitions.get().size();
}

Expected<QStringList> List::get() const
{
	if (!m_partitions)
	{
		Expected<void> result = load();
		if (!result.isOk())
			return result;
	}
	return *m_partitions;
}

Expected<void> List::load() const
{
	char **partitions = guestfs_list_partitions(m_g);

	if (!partitions || !*partitions)
	{
		free(partitions);
		return Expected<void>::fromMessage(IDS_ERR_CANNOT_GET_PART_LIST);
	}

	m_partitions = boost::make_shared<QStringList>();
	for (char **cur = partitions; *cur != NULL; ++cur)
	{
		*m_partitions << *cur;
		free(*cur);
	}
	free(partitions);
	return Expected<void>();
}

} // namespace Partition

////////////////////////////////////////////////////////////
// Ext

int Ext::resize(quint64 newSize) const
{
	return guestfs_resize2fs_size(m_g, QSTR2UTF8(m_partition), newSize);
}

Expected<quint64> Ext::getMinSize(const struct statvfs &stats) const
{
#ifdef NEW_GUESTFS
	qint64 ret = guestfs_vfs_minimum_size(m_partition);
	if (ret < 0)
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_GET_MIN_SIZE, ret);
	return ret;
#else
	char a1[] = "resize2fs", a2[] = "-P";
	QByteArray ba = m_partition.toUtf8();
	char *cmd[] = {a1, a2, ba.data(), NULL};
	char *ret = guestfs_debug(m_g, "sh", cmd);
	QString output(ret);
	free(ret);

	QRegExp sizeRE("Estimated minimum size of the filesystem:\\s+(\\d+)\\s*");
	if (sizeRE.indexIn(output) == -1)
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_PARSE_MIN_SIZE);
	quint64 blocks = sizeRE.cap(1).toULongLong();
	return blocks * stats.f_frsize;
#endif // NEW_GUESTFS
}

////////////////////////////////////////////////////////////
// Ntfs

int Ntfs::resize(quint64 newSize) const
{
	return guestfs_ntfsresize_opts(
			m_g, QSTR2UTF8(m_partition),
			GUESTFS_NTFSRESIZE_OPTS_SIZE, newSize,
			GUESTFS_NTFSRESIZE_OPTS_FORCE, 1,
			-1);
}

Expected<quint64> Ntfs::getMinSize() const
{
#ifdef NEW_GUESTFS
	qint64 ret = guestfs_vfs_minimum_size(m_partition);
	if (ret < 0)
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_GET_MIN_SIZE, ret);
	return ret;
#else
	quint64 bytes;
	char a1[] = "ntfsresize", a2[] = "--info", a3[] ="-f", a4[] = "|", a5[] = "tee";
	QByteArray ba = m_partition.toUtf8();
	char *cmd[] = {a1, a2, a3, ba.data(), a4, a5, NULL};
	char *ret = guestfs_debug(m_g, "sh", cmd);
	QString output(ret);
	free(ret);

	QRegExp sizeRE("You might resize at\\s+(\\d+)\\s+bytes");
	if (sizeRE.indexIn(output) != -1)
		bytes = sizeRE.cap(1).toULongLong();
	else
	{
		QRegExp vSizeRE("Current volume size\\s*:\\s*(\\d+)\\s+bytes");
		if (vSizeRE.indexIn(output) == -1)
			return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_PARSE_MIN_SIZE);
		quint64 volumeSize = vSizeRE.cap(1).toULongLong();
		QRegExp cSizeRE("Cluster size\\s*:\\s*(\\d+)\\s+bytes");
		if (cSizeRE.indexIn(output) == -1)
			return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_PARSE_MIN_SIZE);
		quint64 clusterSize = cSizeRE.cap(1).toULongLong();
		bytes = ceilTo(volumeSize, clusterSize);
	}
	return bytes;
#endif // NEW_GUESTFS
}

////////////////////////////////////////////////////////////
// Btrfs

int Btrfs::resize(quint64 newSize) const
{
	int ret;
   	if ((ret = guestfs_mount(m_g, QSTR2UTF8(m_partition), "/")))
		return ret;
	ret = guestfs_btrfs_filesystem_resize(
				m_g, "/",
				GUESTFS_BTRFS_FILESYSTEM_RESIZE_SIZE, newSize,
				-1);
	guestfs_umount(m_g, "/");
	return ret;
}

Expected<quint64> Btrfs::getMinSize() const
{
#ifdef NEW_GUESTFS
	int result;
	if ((result = guestfs_mount_ro(m_g, QSTR2UTF8(m_partition), "/")))
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_MOUNT, result);

	qint64 ret = guestfs_vfs_minimum_size(m_partition);
	guestfs_umount(m_g, "/");

	if (ret < 0)
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_GET_MIN_SIZE, ret);
	return ret;
#else
	quint64 bytes;
#ifndef OLD_BTRFS_PROGS // btrfs-progs >= 4.2
	int result;
	if ((result = guestfs_mount_ro(m_g, QSTR2UTF8(m_partition), "/")))
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_MOUNT, result);

	char a1[] = "btrfs", a2[] = "inspect-internal", a3[] = "min-dev-size", a4[] = "/sysroot/";
	char *cmd[] = {a1, a2, a3, a4, NULL};
	char *ret = guestfs_debug(m_g, "sh", cmd);
	QString output(ret);
	free(ret);

	guestfs_umount(m_g, "/");

	QRegExp sizeRE("^([\\d]+) bytes");
	if (sizeRE.indexIn(output) != -1)
		bytes = sizeRE.cap(1).toULongLong();
	else
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_PARSE_MIN_SIZE);
#else
	char a1[] = "btrfs", a2[] = "filesystem", a3[] = "show";
	QByteArray ba = partition.toUtf8();
	char *cmd[] = {a1, a2, a3, ba.data(), NULL};
	char *ret = guestfs_debug(m_g, "sh", cmd);
	QString output(ret);
	free(ret);

	QRegExp sizeRE(QString("devid .* used ([\\d\\.]+)([KMGTP])iB path %1").arg(partition));
	if (sizeRE.indexIn(output) != -1)
		bytes = convertToBytes(sizeRE.cap(1), sizeRE.cap(2)[0]);
	else
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_PARSE_MIN_SIZE);
#endif // OLD_BTRFS_PROGS
	return bytes;
#endif // NEW_GUESTFS
}

////////////////////////////////////////////////////////////
// Xfs

int Xfs::resize() const
{
	int ret;
   	if ((ret = guestfs_mount(m_g, QSTR2UTF8(m_partition), "/")))
		return ret;
	ret = guestfs_xfs_growfs(m_g, "/",-1);
	guestfs_umount(m_g, "/");
	return ret;
}

Expected<quint64> Xfs::getMinSize() const
{
	int ret;
   	if ((ret = guestfs_mount_ro(m_g, QSTR2UTF8(m_partition), "/")))
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_MOUNT);
#ifdef NEW_GUESTFS
	qint64 bytes = guestfs_vfs_minimum_size(m_partition);
	guestfs_umount(m_g, "/");
	if (bytes < 0)
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_GET_MIN_SIZE, bytes);
	return bytes;;
#else
	// XFS does NOT support shrinking.
	struct guestfs_xfsinfo *info = guestfs_xfs_info(m_g, "/");
	guestfs_umount(m_g, "/");

	if (NULL == info)
		return Expected<quint64>::fromMessage("Unable to get filesystem info");

	quint64 bytes = info->xfs_blocksize * info->xfs_datablocks;
	free(info);
	return bytes;
#endif // NEW_GUESTFS
}

////////////////////////////////////////////////////////////
// Wrapper

Expected<Wrapper>
Wrapper::create(const QString& filename, const boost::optional<Action> &gfsAction)
{
	boost::shared_ptr<guestfs_h> g;
	if (!(g = boost::shared_ptr<guestfs_h>(guestfs_create(), HandleDestroyer())))
		return Expected<Wrapper>::fromMessage("Unable to create guestfs handle");
	if (guestfs_add_drive(g.get(), QSTR2UTF8(filename)))
		return Expected<Wrapper>::fromMessage("Unable to add drive");
	if (guestfs_launch(g.get()))
		return Expected<Wrapper>::fromMessage("Unable to launch guestfs");

	return Wrapper(g, gfsAction, false);
}

Expected<Wrapper>
Wrapper::createReadOnly(const QString& filename, const boost::optional<Action> &gfsAction)
{
	boost::shared_ptr<guestfs_h> g;
	if (!(g = boost::shared_ptr<guestfs_h>(guestfs_create(), HandleDestroyer())))
		return Expected<Wrapper>::fromMessage("Unable to create guestfs handle");
	if (guestfs_add_drive_ro(g.get(), QSTR2UTF8(filename)))
		return Expected<Wrapper>::fromMessage("Unable to add drive");
	if (guestfs_launch(g.get()))
		return Expected<Wrapper>::fromMessage("Unable to launch guestfs");

	return Wrapper(g, gfsAction, true);
}

Expected<Partition::Stats>
Wrapper::getPartitionStats(const QString &partition) const
{
	int num = guestfs_part_to_partnum(m_g.get(), QSTR2UTF8(partition));
	if (num < 0)
	{
		return Expected<Partition::Stats>::fromMessage(
				QString("Partition %1 not found").arg(partition));
	}

	guestfs_partition_list* list = guestfs_part_list(m_g.get(), GUESTFS_DEVICE);
	if (NULL == list)
	{
		return Expected<Partition::Stats>::fromMessage(
				QString(IDS_ERR_CANNOT_GET_PART_LIST));
	}

	Partition::Stats stats;
	for (unsigned i = 0; i < list->len; ++i)
	{
		if (list->val[i].part_num == num)
		{
			stats.start = list->val[i].part_start;
			stats.end = list->val[i].part_end;
			stats.size = list->val[i].part_size;
			break;
		}
	}

	guestfs_free_partition_list(list);
	return stats;
}

Expected<quint64> Wrapper::getVirtResizeOverhead() const
{
	Expected<QString> firstPart = m_partList.getFirst();
	if (!firstPart.isOk())
		return firstPart;
	Expected<Partition::Stats> firstPartStats =
		getPartitionStats(firstPart.get());
	if (!firstPartStats.isOk())
		return firstPartStats;
	quint64 firstPartStart = firstPartStats.get().start;
	Expected<quint64> sectorSize = getSectorSize();
	if (!sectorSize.isOk())
		return sectorSize;
	quint64 startOverheadSects = qMax(firstPartStart / sectorSize.get(),
									  (quint64) qMax((unsigned)MAX_BOOTLOADER_SECTS, (unsigned)GPT_START_SECTS));
	Expected<int> partCount = m_partList.getCount();
	if (!partCount.isOk())
		return partCount;
	quint64 alignmentSects = (partCount.get() + 1) * ALIGNMENT_SECTS;
	quint64 overhead = startOverheadSects + alignmentSects + GPT_END_SECTS;
	return ceilToMb(overhead * sectorSize.get());
}

Expected<quint64> Wrapper::getPartitionMinSize(const QString &partition) const
{
	Expected<QString> fs = getPartitionFS(partition);
	if (!fs.isOk())
		return fs;
	return getPartitionMinSize(partition, fs.get());
}

Expected<void> Wrapper::shrinkFilesystem(
		const QString &partition, quint64 dec) const
{
	Logger::info(QString("Shrinking FS on %1 by %2").arg(partition).arg(dec));
	Expected<Partition::Stats> partStats = getPartitionStats(partition);
	if (!partStats.isOk())
		return partStats;

	quint64 oldSize = partStats.get().size;
	if (oldSize < dec)
		return Expected<void>::fromMessage("Unable to resize partition below 0");
	quint64 newSize = oldSize - dec;
	Expected<QString> fs = getPartitionFS(partition);
	if (!fs.isOk())
		return fs;
	return shrinkFilesystem(partition, newSize, fs.get());
}

Expected<quint64> Wrapper::getBlockSize() const
{
	int ret = guestfs_blockdev_getbsz(m_g.get(), GUESTFS_DEVICE);
	if (ret < 0)
		return Expected<quint64>::fromMessage("Unable to get block size");
	return ret;
}

Expected<struct statvfs>
Wrapper::getFilesystemStats(const QString &partition) const
{
	int ret;
	if ((ret = guestfs_mount_ro(m_g.get(), QSTR2UTF8(partition), "/")))
		return Expected<struct statvfs>::fromMessage(IDS_ERR_CANNOT_MOUNT, ret);

	struct guestfs_statvfs *g_stat = guestfs_statvfs(m_g.get(), "/");

	guestfs_umount(m_g.get(), "/");

	struct statvfs stat;
	if (g_stat)
	{
		stat.f_bsize = g_stat->bsize;
		stat.f_frsize = g_stat->frsize;
		stat.f_blocks = g_stat->blocks;
		stat.f_bfree = g_stat->bfree;
		stat.f_bavail = g_stat->bavail;
		stat.f_files = g_stat->files;
		stat.f_ffree = g_stat->ffree;
		stat.f_favail = g_stat->favail;
		stat.f_fsid = g_stat->fsid;
		stat.f_flag = g_stat->flag;
		stat.f_namemax = g_stat->namemax;
	}

	if (!g_stat)
		return Expected<struct statvfs>::fromMessage("Unable to get filesystem stats");

	guestfs_free_statvfs(g_stat);
	return stat;
}

Expected<void> Wrapper::shrinkFilesystem(
		const QString &partition, quint64 newSize, const QString &fs) const
{
	int ret;
	Expected<quint64> minSize = getPartitionMinSize(partition, fs);
	if (!minSize.isOk())
		return minSize;
	if (minSize.get() > newSize)
		return Expected<void>::fromMessage(QString(IDS_ERR_NO_FS_FREE_SPACE)
							   .arg(newSize).arg(minSize.get()).arg(minSize.get() - newSize));

	if (fs == "ext2" || fs == "ext3" || fs == "ext4")
	{
		Logger::info(QString("resize2fs %1 %2").arg(partition).arg(newSize));
		ret = (bool)m_gfsAction ? m_gfsAction->get<Ext>(m_g.get(), partition).resize(newSize) : 0;
	}
	else if (fs == "ntfs")
	{
		Logger::info(QString("ntfsresize -f %1 --size %2").arg(partition).arg(newSize));
		ret = (bool)m_gfsAction ? m_gfsAction->get<Ntfs>(m_g.get(), partition).resize(newSize) : 0;
	}
	else if (fs == "btrfs")
	{
		Logger::info(QString("btrfs filesystem resize %1 /").arg(newSize));
		ret = (bool)m_gfsAction ? m_gfsAction->get<Btrfs>(m_g.get(), partition).resize(newSize) : 0;
	}
	else if (fs == "xfs")
	{
		Logger::info(QString("xfs_growfs -d /").arg(newSize));
		ret = (bool)m_gfsAction ? m_gfsAction->get<Xfs>(m_g.get(), partition).resize() : 0;
	}
	else
		return Expected<void>::fromMessage(QString(IDS_ERR_FS_UNSUPPORTED).arg(fs));

	if (ret)
		return Expected<void>::fromMessage("Filesystem resize failed", ret);
	return Expected<void>();
}

Expected<QString>
Wrapper::getPartitionFS(const QString &partition) const
{
	char **filesystems = guestfs_list_filesystems(m_g.get());
	if (!filesystems)
		return Expected<QString>::fromMessage(IDS_ERR_CANNOT_GET_PART_FS);

	QString fs;
	for (char **cur = filesystems; *cur != NULL; cur += 2)
	{
		if (partition == *cur)
			fs = *(cur + 1);
		free(*cur);
		free(*(cur + 1));
	}
	free(filesystems);

	if (fs.isEmpty())
		return Expected<QString>::fromMessage(IDS_ERR_CANNOT_GET_PART_FS);

	return fs;
}

Expected<quint64> Wrapper::getPartitionMinSize(
		const QString &partition, const QString &fs) const
{
	if (fs == "ext2" || fs == "ext3" || fs == "ext4")
	{
		Expected<struct statvfs> stats = getFilesystemStats(partition);
		if (!stats.isOk())
			return stats;
		return Ext(m_g.get(), partition).getMinSize(stats.get());
	}
	else if (fs == "ntfs")
		return Ntfs(m_g.get(), partition).getMinSize();
	else if (fs == "btrfs")
		return Btrfs(m_g.get(), partition).getMinSize();
	else if (fs == "xfs")
		return Xfs(m_g.get(), partition).getMinSize();
	else
		return Expected<quint64>::fromMessage(QString(IDS_ERR_FS_UNSUPPORTED).arg(fs));
}

Expected<quint64> Wrapper::getSectorSize() const
{
	qint64 ret = guestfs_blockdev_getss(m_g.get(), GUESTFS_DEVICE);
	if (ret < 0)
		return Expected<quint64>::fromMessage("Unable to get sector size");
	return ret;
}


