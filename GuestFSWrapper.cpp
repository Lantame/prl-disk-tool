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
#include "Errors.h"

using namespace GuestFS;

namespace
{

const char GUESTFS_DEVICE[] = "/dev/sda";

enum {MAX_BOOTLOADER_SECTS = 4096};
enum {GPT_START_SECTS = 64};
enum {GPT_END_SECTS = 64};
enum {ALIGNMENT_SECTS = 128};
enum {MAX_MBR_PRIMARY = 4};

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

int getPartIndex(const QString &partition)
{
	QString tmp(partition);
	bool ok;
	int partIndex = tmp.remove(GUESTFS_DEVICE).toInt(&ok);
	Q_ASSERT(ok);
	return partIndex;
}

} // namespace

namespace Visitor
{

////////////////////////////////////////////////////////////
// MinSize

struct MinSize: boost::static_visitor<Expected<quint64> >
{
	MinSize(guestfs_h *g, const QString &name, const Helper &helper):
		m_g(g), m_name(name), m_helper(helper)
	{
	}

	template <class T>
	Expected<quint64> operator() (const T &fs) const;

private:
	guestfs_h *m_g;
	QString m_name;
	Helper m_helper;
};

template<> Expected<quint64> MinSize::operator() (const Ext &fs) const
{
	Q_UNUSED(fs);
	Expected<struct statvfs> stats = m_helper.getFilesystemStats(m_name);
	if (!stats.isOk())
		return stats;
	return Ext(m_g, m_name).getMinSize(stats.get());
}

template<> Expected<quint64> MinSize::operator() (const Ntfs &fs) const
{
	Q_UNUSED(fs);
	return Ntfs(m_g, m_name).getMinSize();
}

template<> Expected<quint64> MinSize::operator() (const Btrfs &fs) const
{
	Q_UNUSED(fs);
	return Btrfs(m_g, m_name).getMinSize();
}

template<> Expected<quint64> MinSize::operator() (const Xfs &fs) const
{
	Q_UNUSED(fs);
	return Xfs(m_g, m_name).getMinSize();
}

template<class T> Expected<quint64> MinSize::operator() (const T &fs) const
{
	Q_UNUSED(fs);
	return Expected<quint64>::fromMessage(
				QString(IDS_ERR_FS_UNSUPPORTED), ERR_UNSUPPORTED_FS);
}

////////////////////////////////////////////////////////////
// Resize

struct Resize: boost::static_visitor<Expected<void> >
{
	Resize(guestfs_h *g, const QString &name, quint64 newSize,
		   const boost::optional<Action> &gfsAction):
		m_g(g), m_name(name), m_newSize(newSize), m_gfsAction(gfsAction)
	{
	}

	template <class T>
	Expected<void> operator() (const T &fs) const
	{
		Q_UNUSED(fs);
		Expected<int> ret = execute<T>();
		if (!ret.isOk())
			return ret;
		if (ret.get())
			return Expected<void>::fromMessage("Filesystem resize failed", ret.get());
		return Expected<void>();
	}

private:
	template <class T> Expected<int> execute() const;

	guestfs_h *m_g;
	QString m_name;
	quint64 m_newSize;
	boost::optional<Action> m_gfsAction;
};

template<> Expected<int> Resize::execute<Ext>() const
{
	Logger::info(QString("resize2fs %1 %2").arg(m_name).arg(m_newSize));
	return (bool)m_gfsAction ? m_gfsAction->get<Ext>(m_g, m_name).resize(m_newSize) : 0;
}

template<> Expected<int> Resize::execute<Ntfs>() const
{
	Logger::info(QString("resize2fs %1 %2").arg(m_name).arg(m_newSize));
	return (bool)m_gfsAction ? m_gfsAction->get<Ntfs>(m_g, m_name).resize(m_newSize) : 0;
}

template<> Expected<int> Resize::execute<Btrfs>() const
{
	Logger::info(QString("btrfs filesystem resize %1 /").arg(m_newSize));
	return (bool)m_gfsAction ? m_gfsAction->get<Btrfs>(m_g, m_name).resize(m_newSize) : 0;
}

template<> Expected<int> Resize::execute<Xfs>() const
{
	Logger::info(QString("xfs_growfs -d /"));
	return (bool)m_gfsAction ? m_gfsAction->get<Xfs>(m_g, m_name).resize() : 0;
}

template<class T> Expected<int> Resize::execute() const
{
	return Expected<int>::fromMessage(
				QString(IDS_ERR_FS_UNSUPPORTED), ERR_UNSUPPORTED_FS);
}

////////////////////////////////////////////////////////////
// Same

template <class T>
struct Same: boost::static_visitor<bool>
{
	bool operator() (const T &fs) const;

	template <class U>
	bool operator() (const U &fs) const;
};

template <class T>
bool Same<T>::operator() (const T &fs) const
{
	Q_UNUSED(fs);
	return true;
}

template <class T>
template <class U>
bool Same<T>::template operator() (const U &fs) const
{
	Q_UNUSED(fs);
	return false;
}

} // namespace Visitor

namespace Partition
{

////////////////////////////////////////////////////////////
// Unit

Expected<bool> Unit::isLogical() const
{
	Expected<QString> partTable = m_helper.getPartitionTable();
	if (!partTable.isOk())
		return partTable;
	if (partTable.get() == "gpt")
	{
		// Logical partitions exist only on MBR.
		return false;
	}

	return getPartIndex(m_name) > MAX_MBR_PRIMARY;
}

Expected<bool> Unit::isExtended() const
{
	Expected<QString> partTable = m_helper.getPartitionTable();
	if (!partTable.isOk())
		return partTable;
	if (partTable.get() == "gpt")
	{
		// Extended partitions exist only on MBR.
		return false;
	}

	Expected<Attribute::Aggregate> attrs = getAttributes();
	if (!attrs.isOk())
		return attrs;

	return attrs.get().isExtended();
}

Expected<Stats> Unit::getStats() const
{
	int num = getIndex();
	guestfs_partition_list* list = guestfs_part_list(m_g, GUESTFS_DEVICE);
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

Expected<quint64> Unit::getMinSize() const
{
	Expected<fs_type> fs = getFS();
	if (!fs.isOk())
		return fs;
	return getMinSize(fs.get());
}

Expected<quint64> Unit::getMinSize(const fs_type &fs) const
{
	return boost::apply_visitor(Visitor::MinSize(m_g, m_name, m_helper), fs);
}

Expected<fs_type> Unit::getFS() const
{
	char **filesystems = guestfs_list_filesystems(m_g);
	if (!filesystems)
		return Expected<fs_type>::fromMessage(IDS_ERR_CANNOT_GET_PART_FS);

	QString fs;
	for (char **cur = filesystems; *cur != NULL; cur += 2)
	{
		if (m_name == *cur)
			fs = *(cur + 1);
		free(*cur);
		free(*(cur + 1));
	}
	free(filesystems);

	if (fs.isEmpty())
	{
		// guestfs does not knwon such partition
		return Expected<fs_type>::fromMessage(
				IDS_ERR_CANNOT_GET_PART_FS);
	}

	if (fs == "ext2" || fs == "ext3" || fs == "ext4")
		return fs_type(Ext());
	else if (fs == "ntfs")
		return fs_type(Ntfs());
	else if (fs == "btrfs")
		return fs_type(Btrfs());
	else if (fs == "xfs")
		return fs_type(Xfs());
	else
		return fs_type(Unknown());
}

Expected<struct statvfs> Unit::getFilesystemStats() const
{
	return m_helper.getFilesystemStats(m_name);
}

Expected<void> Unit::shrinkFilesystem(quint64 dec) const
{
	Logger::info(QString("Shrinking FS on %1 by %2").arg(m_name).arg(dec));
	Expected<Stats> partStats = getStats();
	if (!partStats.isOk())
		return partStats;

	quint64 oldSize = partStats.get().size;
	if (oldSize < dec)
		return Expected<void>::fromMessage("Unable to resize m_name below 0");
	quint64 newSize = oldSize - dec;
	return resizeFilesystem(newSize);
}

Expected<void> Unit::resizeFilesystem(quint64 newSize) const
{
	Expected<fs_type> fs = getFS();
	if (!fs.isOk())
		return fs;
	return resizeFilesystem(newSize, fs.get());
}

Expected<void> Unit::resizeFilesystem(quint64 newSize, const fs_type &fs) const
{
	Expected<quint64> minSize = getMinSize(fs);
	if (!minSize.isOk())
		return minSize;
	if (minSize.get() > newSize)
		return Expected<void>::fromMessage(QString(IDS_ERR_NO_FS_FREE_SPACE)
							   .arg(newSize).arg(minSize.get()).arg(minSize.get() - newSize));

	return boost::apply_visitor(Visitor::Resize(m_g, m_name, newSize, m_gfsAction), fs);
}

Expected<bool> Unit::isFilesystemSupported() const
{
	Expected<fs_type> fs = getFS();
	if (!fs.isOk())
		return fs;
	return !boost::apply_visitor(Visitor::Same<Unknown>(), fs.get());
}

int Unit::getIndex() const
{
	return getPartIndex(m_name);
}

template<>
Expected<Attribute::Gpt> Unit::getAttributesInternal() const
{
	char *name = guestfs_part_get_name(m_g, GUESTFS_DEVICE, getIndex());
	if (!name)
	{
		return Expected<Attribute::Gpt>::fromMessage(
				"Unable to get GPT partition name");
	}

	char *gptType = guestfs_part_get_gpt_type(
			m_g, GUESTFS_DEVICE, getIndex());
	if (!gptType)
		return Expected<Attribute::Gpt>::fromMessage("Unable to get GPT type");

	Attribute::Gpt attrs(name, gptType);
	free(name);
	free(gptType);
	return attrs;
}

template<>
Expected<Attribute::Mbr> Unit::getAttributesInternal() const
{
	int ret = guestfs_part_get_mbr_id(m_g, GUESTFS_DEVICE, getIndex());
	if (ret == -1)
		return Expected<Attribute::Mbr>::fromMessage("Unable to get mbr id");
	Attribute::Mbr attrs(ret);

	// libguestfs fails on this condition.
	if (!attrs.isExtended())
	{
		char *gptType = guestfs_part_get_gpt_type(
				m_g, GUESTFS_DEVICE, getIndex());
		if (!gptType)
			return Expected<Attribute::Mbr>::fromMessage("Unable to get GPT type");
		attrs.m_gptType = gptType;
		free(gptType);
	}
	return attrs;
}

Expected<Attribute::Aggregate> Unit::getAttributes() const
{
	int ret = guestfs_part_get_bootable(m_g, GUESTFS_DEVICE, getIndex());
	if (ret == -1)
		return Expected<Attribute::Aggregate>::fromMessage("Unable to get bootable flag");
	bool bootable = (ret != 0);

	Expected<QString> partTable = m_helper.getPartitionTable();
	if (!partTable.isOk())
		return partTable;

	if (partTable.get() == "msdos")
	{
		Expected<Attribute::Mbr> attrs = getAttributesInternal<Attribute::Mbr>();
		if (!attrs.isOk())
			return attrs;
		return Attribute::Aggregate(bootable, attrs.get());
	}
	else // gpt
	{
		Expected<Attribute::Gpt> attrs = getAttributesInternal<Attribute::Gpt>();
		if (!attrs.isOk())
			return attrs;
		return Attribute::Aggregate(bootable, attrs.get());
	}
}

Expected<void> Unit::apply(const Attribute::Aggregate &attrs) const
{
	int ret;
	if ((ret = guestfs_part_set_bootable(
					m_g, GUESTFS_DEVICE, getIndex(), attrs.isBootable())))
		return Expected<void>::fromMessage("Unable to set bootable flag", ret);

	Expected<QString> partTable = m_helper.getPartitionTable();
	if (!partTable.isOk())
		return partTable;

	if (partTable.get() == "msdos")
	{
		boost::optional<Attribute::Mbr> typedAttrs = attrs.get<Attribute::Mbr>();
		if (!typedAttrs)
			return Expected<void>::fromMessage("Invalid attrs type");

		if ((ret = guestfs_part_set_mbr_id(
						m_g, GUESTFS_DEVICE, getIndex(), typedAttrs->m_mbrId)))
			return Expected<void>::fromMessage("Unable to set mbr id", ret);
	}
	else // gpt
	{
		boost::optional<Attribute::Gpt> typedAttrs = attrs.get<Attribute::Gpt>();
		if (!typedAttrs)
			return Expected<void>::fromMessage("Invalid attrs type");

		if ((ret = guestfs_part_set_name(
						m_g, GUESTFS_DEVICE, getIndex(), QSTR2UTF8(typedAttrs->m_name))))
			return Expected<void>::fromMessage("Unable to set m_name name", ret);

		if ((ret = guestfs_part_set_gpt_type(
						m_g, GUESTFS_DEVICE, getIndex(), QSTR2UTF8(typedAttrs->m_gptType))))
			return Expected<void>::fromMessage("Unable to set gpt type", ret);
	}
	return Expected<void>();
}

////////////////////////////////////////////////////////////
// List

Expected<Unit> List::getFirst() const
{
	Expected<QList<Unit> > partitions = get();
	if (!partitions.isOk())
		return partitions;
	if (partitions.get().isEmpty())
	{
		return Expected<QString>::fromMessage(
				"No partitions found", ERR_NO_PARTITIONS);
	}
	return partitions.get().first();
}

Expected<Unit> List::getLast() const
{
	Expected<QList<Unit> > partitions = get();
	if (!partitions.isOk())
		return partitions;
	if (partitions.get().isEmpty())
	{
		return Expected<QString>::fromMessage(
				"No partitions found", ERR_NO_PARTITIONS);
	}
	return partitions.get().last();
}

Expected<int> List::getCount() const
{
	Expected<QList<Unit> > partitions = get();
	if (!partitions.isOk())
		return partitions;
	return partitions.get().size();
}

Expected<QList<Unit> > List::get() const
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

	if (!partitions)
		return Expected<void>::fromMessage(IDS_ERR_CANNOT_GET_PART_LIST);

	m_partitions = boost::make_shared<QList<Unit> >();
	for (char **cur = partitions; *cur != NULL; ++cur)
	{
		*m_partitions << Unit(m_g, m_helper, m_gfsAction, *cur);
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
	// Ext requires size to be multiplier of 1K.
	newSize = newSize / 1024 * 1024;
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
// Helper

Expected<QString> Helper::getPartitionTable() const
{
	char *partTable = guestfs_part_get_parttype(m_g, GUESTFS_DEVICE);
	if (!partTable)
		return Expected<QString>::fromMessage("Unable to get partition table type");

	QString table(partTable);
	free(partTable);

	if (table == "msdos" || table == "gpt")
		return table;

	return Expected<QString>::fromMessage(
			QString("Unknown partition table type: '%1'").arg(table));
}

Expected<struct statvfs> Helper::getFilesystemStats(const QString &name) const
{
	int ret;
	if ((ret = guestfs_mount_ro(m_g, QSTR2UTF8(name), "/")))
		return Expected<struct statvfs>::fromMessage(IDS_ERR_CANNOT_MOUNT, ret);

	struct guestfs_statvfs *g_stat = guestfs_statvfs(m_g, "/");

	guestfs_umount(m_g, "/");

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

Expected<Partition::Unit> Wrapper::getContainer() const
{
	Expected<QList<Partition::Unit> > parts = m_partList.get();
	if (!parts.isOk())
		return parts;
	QList<Partition::Unit> partitions = parts.get();
	if (partitions.isEmpty())
	{
		return Expected<Partition::Unit>::fromMessage(
				"No partitions found", ERR_NO_PARTITIONS);
	}

	Expected<QString> partTable = m_helper.getPartitionTable();
	if (!partTable.isOk())
		return partTable;
	if (partTable.get() == "gpt")
	{
		// We have no extended/logical partitions.
		return Expected<Partition::Unit>::fromMessage(
				"No containers on GPT", ERR_UNSUPPORTED_PARTITION);
	}

	for (int i = 0; i < partitions.length(); ++i)
	{
		// Save 1 call.
		if (partitions[i].getIndex() > MAX_MBR_PRIMARY)
			break;

		Expected<bool> ext = partitions[i].isExtended();
		if (!ext.isOk())
			return ext;

		// One extended at most.
		if (ext.get())
			return partitions[i];
	}
	return Expected<Partition::Unit>::fromMessage("Extended partition not found");
}

Expected<quint64> Wrapper::getVirtResizeOverhead() const
{
	Expected<Partition::Unit> firstPart = m_partList.getFirst();
	if (!firstPart.isOk())
		return firstPart;
	Expected<Partition::Stats> firstPartStats = firstPart.get().getStats();
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

Expected<quint64> Wrapper::getBlockSize() const
{
	int ret = guestfs_blockdev_getbsz(m_g.get(), GUESTFS_DEVICE);
	if (ret < 0)
		return Expected<quint64>::fromMessage("Unable to get block size");
	return ret;
}

Expected<void> Wrapper::expandGPT() const
{
	// move second header
	Logger::info(QString("sgdisk -e %1").arg(GUESTFS_DEVICE));
	if (!m_gfsAction)
		return Expected<void>();

	char a1[] = "sgdisk", a2[] = "-e", a3[] = "-v";
	QByteArray device(GUESTFS_DEVICE);
	char *cmd[] = {a1, a2, device.data(), NULL};
	char *ret = guestfs_debug(m_g.get(), "sh", cmd);
	QString output(ret);
	free(ret);
	// Hack. To make sure the operation completed successfully.
	if (!output.contains("The operation has completed successfully."))
	{
		return Expected<void>::fromMessage(
				QString("sgdisk error. it returned:\n%1").arg(output));
	}

	// verify correctness
	cmd[1] = a3;
	ret = guestfs_debug(m_g.get(), "sh", cmd);
	output = ret;
	free(ret);
	if (!output.contains("No problems found."))
	{
		return Expected<void>::fromMessage(
				QString("sgdisk error. it returned:\n%1").arg(output));
	}

	return Expected<void>();
}

Expected<Wrapper::partMap_type> Wrapper::getLogical() const
{
	Expected<QList<Partition::Unit> > parts = m_partList.get();
	if (!parts.isOk())
		return parts;
	QList<Partition::Unit> partitions = parts.get();

	partMap_type logical;
	for (int i = partitions.length() - 1; i >= 0; --i)
	{
		int partIndex = partitions[i].getIndex();
		// Logical partitions finished.
		if (partIndex <= MAX_MBR_PRIMARY)
			break;

		Expected<Partition::Stats> stats = partitions[i].getStats();
		if (!stats.isOk())
			return stats;

		Expected<Partition::Attribute::Aggregate> attrs = partitions[i].getAttributes();
		if (!attrs.isOk())
			return attrs;

		logical.insert(partIndex, partInfo_type(stats.get(), attrs.get()));
	}
	return logical;
}

Expected<void> Wrapper::createLogical(const Wrapper::partMap_type &logical) const
{
	Expected<quint64> sectorSize = getSectorSize();
	if (!sectorSize.isOk())
		return sectorSize;

	// NB: QMap is sorted by key.
	partMap_type::const_iterator it;
	for (it = logical.constBegin(); it != logical.constEnd(); ++it)
	{
		const Partition::Stats &curStats = it.value().first;
		const Partition::Attribute::Aggregate &curAttrs = it.value().second;

		Logger::info(QString("part-add %1 logical %2 %3")
					 .arg(GUESTFS_DEVICE).arg(curStats.start / sectorSize.get())
					 .arg(curStats.end / sectorSize.get()));
		if (!m_gfsAction)
			continue;

		int ret;
		if ((ret = guestfs_part_add(
						m_g.get(), GUESTFS_DEVICE, "logical",
						curStats.start / sectorSize.get(),
						curStats.end / sectorSize.get())))
			return Expected<void>::fromMessage("Unable to create partition", ret);

		Expected<void> res;
		Partition::Unit part(m_g.get(), m_helper, m_gfsAction,
		                     QString("%1%2").arg(GUESTFS_DEVICE).arg(it.key()));
		if (!(res = part.apply(curAttrs)).isOk())
			return res;
	}
	return Expected<void>();
}

Expected<void> Wrapper::resizePartition(
	   const Partition::Unit &partition, quint64 startSector, quint64 endSector) const
{
	int partIndex = partition.getIndex(), ret;
	Expected<Partition::Attribute::Aggregate> attrs = partition.getAttributes();
	if (!attrs.isOk())
		return attrs;

	// NB: QMap is sorted by key.
	partMap_type logical;
	QString type("primary");
	if (attrs.get().isExtended())
	{
		// Collect inner logical partitions.
		// They will be implicitly deleted on extended partition delete.
		Expected<partMap_type> logicalRes = getLogical();
		if (!logicalRes.isOk())
			return logicalRes;
		logical = logicalRes.get();
		type = "extended";
	}
	else
	{
		Expected<bool> logic = partition.isLogical();
		if (!logic.isOk())
			return logic;
		if (logic.get())
			type = "logical";
	}

	Logger::info(QString("part-del %1 %2").arg(GUESTFS_DEVICE).arg(partIndex));
	if (m_gfsAction && (ret = guestfs_part_del(m_g.get(), GUESTFS_DEVICE, partIndex)))
		return Expected<void>::fromMessage("Unable to delete partition", ret);

	Logger::info(QString("part-add %1 %2 %3 %4")
				 .arg(GUESTFS_DEVICE).arg(type).arg(startSector).arg(endSector));
	if (m_gfsAction && (ret = guestfs_part_add(
					m_g.get(), GUESTFS_DEVICE, QSTR2UTF8(type),
					startSector, endSector)))
		return Expected<void>::fromMessage("Unable to create partition", ret);

	Expected<void> result;
	if (m_gfsAction && !(result = partition.apply(attrs.get())).isOk())
		return result;

	// Restore inner logical partitions, if needed.
	if (attrs.get().isExtended())
	{
		// NB: Due to bug(?) in parted/libguestfs,
		// it fails if start(1st logical) - start(extended) <= 1s.
		if (!(result = createLogical(logical)).isOk())
			return result;
	}

	return Expected<void>();
}

Expected<quint64> Wrapper::getSectorSize() const
{
	qint64 ret = guestfs_blockdev_getss(m_g.get(), GUESTFS_DEVICE);
	if (ret < 0)
		return Expected<quint64>::fromMessage("Unable to get sector size");
	return ret;
}
