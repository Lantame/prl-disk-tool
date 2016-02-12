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
#include <errno.h>

#include "GuestFSWrapper.h"
#include "StringTable.h"
#include "Errors.h"

namespace
{

const char GUESTFS_DEVICE[] = "/dev/sda";

enum {MAX_BOOTLOADER_SECTS = 4096};
enum {GPT_START_SECTS = 64};
enum {GPT_END_SECTS = 64};
enum {ALIGNMENT_SECTS = 128};
enum {MAX_MBR_PRIMARY = 4};
enum {MIN_SWAP_SIZE = 40 * 1024}; // mkswap asks for 40KiB = 10 pages.
enum {LVM_METADATA_SIZE = 14336}; // In sectors, taken from previous version.

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

GuestFS::fs_type parseFilesystem(const QString &fs)
{
	if (fs == "ext2" || fs == "ext3" || fs == "ext4")
		return GuestFS::fs_type(GuestFS::Ext());
	else if (fs == "ntfs")
		return GuestFS::fs_type(GuestFS::Ntfs());
	else if (fs == "btrfs")
		return GuestFS::fs_type(GuestFS::Btrfs());
	else if (fs == "xfs")
		return GuestFS::fs_type(GuestFS::Xfs());
	else if (fs == "swap")
		return GuestFS::fs_type(GuestFS::Swap());
	return GuestFS::fs_type(GuestFS::Unknown());
}

} // namespace

namespace GuestFS
{

namespace Mode
{

////////////////////////////////////////////////////////////
// Shrink

struct Shrink
{
};

////////////////////////////////////////////////////////////
// Expand

struct Expand
{
};

typedef boost::variant<Shrink, Expand> mode_type;

mode_type get(quint64 newSize, quint64 pvSize)
{
	return newSize < pvSize ? mode_type(Shrink()) : mode_type(Expand());
}

} // namespace Mode

namespace Visitor
{

////////////////////////////////////////////////////////////
// MinSize

struct MinSize: boost::static_visitor<Expected<quint64> >
{
	MinSize(guestfs_h *g, const QString &name,
			const boost::optional<Action> &gfsAction):
		m_g(g), m_name(name), m_gfsAction(gfsAction)
	{
	}

	template <class T>
	Expected<quint64> operator() (const T &fs) const;

private:
	guestfs_h *m_g;
	QString m_name;
	boost::optional<Action> m_gfsAction;
};

template<> Expected<quint64> MinSize::operator() (const Swap &fs) const
{
	Q_UNUSED(fs);
	return Swap::getMinSize();
}

template<> Expected<quint64> MinSize::operator() (const Unknown &fs) const
{
	Q_UNUSED(fs);
	return Expected<quint64>::fromMessage(
			QString(IDS_ERR_FS_UNSUPPORTED), ERR_UNSUPPORTED_FS);
}

template<> Expected<quint64> MinSize::operator() (const Volume::Physical &fs) const
{
	return Volume::Physical(fs.getPhysical(), m_g, m_name, m_gfsAction).getMinSize();
}

template<class T> Expected<quint64> MinSize::operator() (const T &fs) const
{
	Q_UNUSED(fs);
	return T(m_g, m_name).getMinSize();
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

template<> Expected<void> Resize::operator() (const Volume::Physical &fs) const
{
	return Volume::Physical(fs.getPhysical(), m_g, m_name, m_gfsAction).resize(m_newSize);
}

template<> Expected<int> Resize::execute<Ext>() const
{
	Logger::info(QString("resize2fs %1 %2").arg(m_name).arg(m_newSize));
	return (bool)m_gfsAction ? m_gfsAction->get<Ext>(m_g, m_name).resize(m_newSize) : 0;
}

template<> Expected<int> Resize::execute<Ntfs>() const
{
	Logger::info(QString("ntfsresize -f %1 --size %2").arg(m_name).arg(m_newSize));
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

template<> Expected<int> Resize::execute<Swap>() const
{
	Logger::info(QString("swap resize (ignore)"));
	return 0;
}

template<class T> Expected<int> Resize::execute() const
{
	return Expected<int>::fromMessage(
				QString(IDS_ERR_FS_UNSUPPORTED), ERR_UNSUPPORTED_FS);
}

////////////////////////////////////////////////////////////
// LVDelta

struct LVDelta: boost::static_visitor<Expected<qint64> >
{
	LVDelta(const Volume::Physical &physical, quint64 newSize,
			const Lvm::Segment &lastSegment):
		m_physical(physical), m_newSize(newSize), m_lastSegment(lastSegment)
	{
	}

	template <class T>
	Expected<qint64> operator() (const T &mode) const
	{
		return m_physical.getLVDelta(m_newSize, m_lastSegment, mode);
	}

private:
	const Volume::Physical &m_physical;
	quint64 m_newSize;
	Lvm::Segment m_lastSegment;
};

////////////////////////////////////////////////////////////
// ResizePV

struct ResizePV: boost::static_visitor<Expected<void> >
{
	ResizePV(const Volume::Physical &physical, quint64 newSize,
			 qint64 lvDelta, const Volume::Logical &logical):
		m_physical(physical), m_newSize(newSize),
		m_lvDelta(lvDelta), m_logical(logical)
	{
	}

	template <class T>
	Expected<void> operator() (const T &mode) const;

private:
	template <class T>
	Expected<void> resize(const T &mode, const Partition::Unit &lv,
						  quint64 lvNewSize) const;

	const Volume::Physical &m_physical;
	quint64 m_newSize;
	qint64 m_lvDelta;
	Volume::Logical m_logical;
};

template <class T>
Expected<void> ResizePV::operator() (const T &mode) const
{
	Expected<quint64> lvSize = m_logical.getSize();
	if (!lvSize.isOk())
		return lvSize;
	quint64 lvNewSize = lvSize.get() + m_lvDelta;

	Expected<Partition::Unit> lv = m_logical.createUnit();
	if (!lv.isOk())
		return lv;
	return resize(mode, lv.get(), lvNewSize);
}

template<> Expected<void> ResizePV::resize(
		const Mode::Shrink &mode, const Partition::Unit &lv,
		quint64 lvNewSize) const
{
	Q_UNUSED(mode);
	Expected<void> res;
	if (!(res = lv.resizeContent(lvNewSize)).isOk())
		return res;
	if (!(res = m_logical.resize(lvNewSize)).isOk())
		return res;
	return m_physical.pvresize(m_newSize);
}

template<> Expected<void> ResizePV::resize(
		const Mode::Expand &mode, const Partition::Unit &lv,
		quint64 lvNewSize) const
{
	Q_UNUSED(mode);
	Expected<void> res;
	if (!(res = m_physical.pvresize(m_newSize)).isOk())
			return res;
	if (!(res = m_logical.resize(lvNewSize)).isOk())
		return res;
	return lv.resizeContent(lvNewSize);
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
	return boost::apply_visitor(Visitor::MinSize(m_g, m_name, m_gfsAction), m_filesystem);
}

Expected<quint64> Unit::getSize() const
{
	return m_helper.getSize64(m_name);
}

Expected<struct statvfs> Unit::getFilesystemStats() const
{
	return m_helper.getFilesystemStats(m_name);
}

Expected<void> Unit::shrinkContent(quint64 dec) const
{
	Logger::info(QString("Shrinking content on %1 by %2").arg(m_name).arg(dec));
	Expected<Stats> partStats = getStats();
	if (!partStats.isOk())
		return partStats;

	quint64 oldSize = partStats.get().size;
	if (oldSize < dec)
	{
		return Expected<void>::fromMessage(
				QString("Unable to resize %1 below 0").arg(m_name));
	}
	quint64 newSize = oldSize - dec;
	return resizeContent(newSize);
}

Expected<void> Unit::resizeContent(quint64 newSize) const
{
	Expected<quint64> minSize = getMinSize();
	if (!minSize.isOk())
		return minSize;
	if (minSize.get() > newSize)
	{
		return Expected<void>::fromMessage(QString(IDS_ERR_NO_FS_FREE_SPACE)
							   .arg(newSize).arg(minSize.get()).arg(minSize.get() - newSize));
	}

	return boost::apply_visitor(Visitor::Resize(m_g, m_name, newSize, m_gfsAction), m_filesystem);
}

Expected<bool> Unit::isFilesystemSupported() const
{
	return getFilesystem<Unknown>() == NULL;
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

	char *gptGuid = guestfs_part_get_gpt_guid(m_g, GUESTFS_DEVICE, getIndex());
	if (!gptGuid)
	{
		return Expected<Attribute::Gpt>::fromMessage(
				"Unable to get GPT partition GUID");
	}

	Attribute::Gpt attrs(name, gptType, gptGuid);
	free(name);
	free(gptType);
	free(gptGuid);
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

		if ((ret = guestfs_part_set_gpt_guid(
						m_g, GUESTFS_DEVICE, getIndex(), QSTR2UTF8(typedAttrs->m_gptGuid))))
			return Expected<void>::fromMessage("Unable to set gpt GUID", ret);
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

Expected<Unit> List::createUnit(const QString &name) const
{
	Expected<fsMap_type> content = getContent();
	if (!content.isOk())
		return content;
	return Unit(m_g, m_gfsAction, name,
		        content.get().value(name, Unknown()));
}

Expected<void> List::load() const
{
	char **partitions = guestfs_list_partitions(m_g);
	if (!partitions)
		return Expected<void>::fromMessage(IDS_ERR_CANNOT_GET_PART_LIST);

	Expected<fsMap_type> content = getContent();
	if (!content.isOk())
		return content;

	QList<Unit> partList;
	for (char **cur = partitions; *cur != NULL; ++cur)
	{
		partList << Unit(m_g, m_gfsAction, *cur,
		                 content.get().value(*cur, Unknown()));
		free(*cur);
	}

	free(partitions);
	m_partitions = partList;
	return Expected<void>();
}

Expected<List::fsMap_type> List::getContent() const
{
	if (m_content)
		return *m_content;

	Expected<fsMap_type> filesystems = getFilesystems();
	if (!filesystems.isOk())
		return filesystems;
	fsMap_type content(filesystems.get());

	// Check LVM
	Helper helper(m_g);
	Expected<QStringList> vgs = helper.getVG().get();
	if (!vgs.isOk())
		return vgs;

	Q_FOREACH(const QString &vg, vgs.get())
	{
		Expected<Lvm::Config> config = helper.getVG().getConfig(vg);
		if (!config.isOk())
			return config;
		QStringList pvs = config.get().getPhysicals();
		Q_FOREACH(const QString &pv, pvs)
			content.insert(pv, Volume::Physical(config.get().getPhysical(pv)));
	}
	m_content = content;
	return *m_content;
}

Expected<List::fsMap_type> List::getFilesystems() const
{
	char **filesystems = guestfs_list_filesystems(m_g);
	if (!filesystems)
		return Expected<fs_type>::fromMessage(IDS_ERR_CANNOT_GET_PART_FS);

	fsMap_type result;
	for (char **cur = filesystems; *cur != NULL; cur += 2)
	{
		result.insert(*cur, parseFilesystem(*(cur + 1)));
		free(*cur);
		free(*(cur + 1));
	}

	free(filesystems);
	return result;
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

Expected<quint64> Ext::getMinSize() const
{
	qint64 ret = guestfs_vfs_minimum_size(m_g, QSTR2UTF8(m_partition));
	if (ret < 0)
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_GET_MIN_SIZE, ret);
	return ret;
}

////////////////////////////////////////////////////////////
// Ntfs

int Ntfs::resize(quint64 newSize) const
{
	int ret = guestfs_ntfsresize_opts(
			m_g, QSTR2UTF8(m_partition),
			GUESTFS_NTFSRESIZE_OPTS_SIZE, newSize,
			GUESTFS_NTFSRESIZE_OPTS_FORCE, 1,
			-1);
	if (ret)
	{
		Logger::error("NTFS resize failed. Probably the filesystem was not unmounted cleanly.\n\
		               Please try to reboot Windows and/or run CHKDSK /F.");
		return ret;
	}
	return guestfs_ntfsfix(m_g, QSTR2UTF8(m_partition), -1);
}

Expected<quint64> Ntfs::getMinSize() const
{
	qint64 ret = guestfs_vfs_minimum_size(m_g, QSTR2UTF8(m_partition));
	if (ret < 0)
	{
		Logger::error("Failed to get NTFS minimum size.\n"
		              "Probably the filesystem was not unmounted cleanly.\n"
		              "Please try to reboot Windows and/or run CHKDSK /F.");
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_GET_MIN_SIZE, ret);
	}
	return ret;
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
	int result;
	if ((result = guestfs_mount_ro(m_g, QSTR2UTF8(m_partition), "/")))
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_MOUNT, result);

	qint64 ret = guestfs_vfs_minimum_size(m_g, QSTR2UTF8(m_partition));
	guestfs_umount(m_g, "/");

	if (ret < 0)
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_GET_MIN_SIZE, ret);
	return ret;
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
	qint64 bytes = guestfs_vfs_minimum_size(m_g, QSTR2UTF8(m_partition));
	guestfs_umount(m_g, "/");
	if (bytes < 0)
		return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_GET_MIN_SIZE, bytes);
	return bytes;;
}

////////////////////////////////////////////////////////////
// Swap

quint64 Swap::getMinSize()
{
	return MIN_SWAP_SIZE;
}


namespace Volume
{

////////////////////////////////////////////////////////////
// Logical

Expected<quint64> Logical::getSize() const
{
	return Helper(m_g).getSize64(m_fullName);
}

Expected<Partition::Unit> Logical::createUnit() const
{
	return Partition::List(m_g, m_gfsAction).createUnit(m_fullName);
}

Expected<quint64> Logical::getMinSize() const
{
	Expected<quint64> size = getSize();
	if (!size.isOk())
		return size;
	Expected<Partition::Unit> lv = createUnit();
	if (!lv.isOk())
		return lv;
	// Cannot resize unsupported fs.
	quint64 minSize = size.get();
	Expected<quint64> minSizeRes = lv.get().getMinSize();
	if (!minSizeRes.isOk())
	{
		if (minSizeRes.getCode() != ERR_UNSUPPORTED_FS)
			return minSizeRes;
	}
	else
		minSize = minSizeRes.get();
	Logger::info(QString("LV minimum size: %1").arg(minSize));
	return minSize;
}

QString Logical::getName(const Lvm::Group &group, const Lvm::Segment &lastSegment)
{
	return QString("/dev/%1/%2").arg(
			group.getName(),
			lastSegment.getLogical().getName());
}

Expected<void> Logical::resize(quint64 newSize) const
{
	// In MB.
	Logger::info(QString("lvresize %1 %2M").arg(m_fullName).arg(newSize >> 20));
	int ret;
	if ((ret = guestfs_lvresize(m_g, QSTR2UTF8(m_fullName), newSize >> 20)))
		return Expected<void>::fromMessage("Unable to resize LV");
	return Expected<void>();
}

////////////////////////////////////////////////////////////
// Physical

Expected<quint64> Physical::getSize() const
{
	struct guestfs_lvm_pv_list *pvs = guestfs_pvs_full(m_g);
	if (pvs == NULL)
		return Expected<quint64>::fromMessage("Unable to get PVs");

	quint64 size = 0;
	for (int i = 0; i < (int)pvs->len; ++i)
	{
		if (m_partition == pvs->val[i].pv_name)
			size = pvs->val[i].pv_size;
	}
	guestfs_free_lvm_pv_list(pvs);

	if (size == 0)
		return Expected<void>::fromMessage("Unable to get PV size");
	return size;
}

Expected<qint64> Physical::calculateLVDelta(
		quint64 newSize, const Lvm::Segment &lastSegment) const
{
	Helper helper(m_g);
	Expected<quint64> sectorSize = helper.getSectorSize();
	if (!sectorSize.isOk())
		return sectorSize;
	quint64 extentSize = m_physical.getGroup().getExtentSizeInSectors() * sectorSize.get();

	qint64 lvDelta = newSize - LVM_METADATA_SIZE * sectorSize.get()
							 - (lastSegment.getEndInExtents() + 1) * extentSize;
	// Round down to extent size.
	if (lvDelta < 0)
		lvDelta = -((-lvDelta + extentSize - 1) / extentSize * extentSize);
	else
		lvDelta = lvDelta / extentSize * extentSize;
	return lvDelta;
}

template<>
Expected<qint64> Physical::getLVDelta(
		quint64 newSize, const Lvm::Segment &lastSegment,
		const Mode::Shrink &mode) const
{
	Q_UNUSED(mode);
	Expected<qint64> lvDeltaRes = calculateLVDelta(newSize, lastSegment);
	if (!lvDeltaRes.isOk())
		return lvDeltaRes;
	qint64 lvDelta = lvDeltaRes.get();

	/* Adjust lvDelta considering performed operation. */
	if (lvDelta > 0)
		lvDelta = 0;
	else if (lvDelta < 0 && !lastSegment.isResizeable())
	{
		// LV needs resize but it is impossible.
		return Expected<qint64>::fromMessage("Unable to resize LV");
	}

	return lvDelta;
}

template<>
Expected<qint64> Physical::getLVDelta(
		quint64 newSize, const Lvm::Segment &lastSegment,
		const Mode::Expand &mode) const
{
	Q_UNUSED(mode);
	Expected<qint64> lvDeltaRes = calculateLVDelta(newSize, lastSegment);
	if (!lvDeltaRes.isOk())
		return lvDeltaRes;
	qint64 lvDelta = lvDeltaRes.get();

	/* Adjust lvDelta considering performed operation. */
	if (lvDelta < 0)
	{
		return Expected<qint64>::fromMessage(
				"Need LV shrink while expanding PV");
	}
	else if (lvDelta > 0 && !lastSegment.isResizeable())
		lvDelta = 0;

	return lvDelta;
}

Expected<void> Physical::pvresize(quint64 newSize) const
{
	Logger::info(QString("pvresize-size %1 %2").arg(m_partition).arg(newSize));
	int ret;
	if ((ret = guestfs_pvresize_size(m_g, QSTR2UTF8(m_partition), newSize)))
		return Expected<void>::fromMessage("Unable to resize PV");
	return Expected<void>();
}

Expected<quint64> Physical::getMinSize() const
{
	const Lvm::Group& group = m_physical.getGroup();
	if (!group.isResizeable() || !group.isWriteable())
		return Expected<quint64>::fromMessage("VG is not modifiable");

	Helper helper(m_g);
	Expected<quint64> sectorSize = helper.getSectorSize();
	if (!sectorSize.isOk())
		return sectorSize;
	quint64 extentSize = group.getExtentSizeInSectors() * sectorSize.get();
	boost::optional<Lvm::Segment> lastSegment = m_physical.getLastSegment();
	if (!lastSegment)
	{
		// PV is empty. Can resize up to metadata size.
		return LVM_METADATA_SIZE * sectorSize.get();
	}

	// Stripped and not-last segments cannot be resized.
	if (!lastSegment->isResizeable())
	{
		return LVM_METADATA_SIZE * sectorSize.get() +
			   (lastSegment.get().getEndInExtents() + 1) * extentSize;
	}

	// Get minimum size of content.
	QString lvName = Logical::getName(group, *lastSegment);
	Logical logical(m_g, lvName, m_gfsAction);
	Expected<quint64> lvSize = logical.getSize();
	if (!lvSize.isOk())
		return lvSize;
	Expected<quint64> minSize = logical.getMinSize();
	if (!minSize.isOk())
		return minSize;

	// Decrease by last segment size at most.
	quint64 lvResultSize = qMax(minSize.get(), lvSize.get() -
			lastSegment->getSizeInExtents() * extentSize);
	// Roundup to extent size.
	lvResultSize = (lvResultSize + extentSize - 1) / extentSize * extentSize;
	// Metadata header.
	quint64 pvMinSize = LVM_METADATA_SIZE * sectorSize.get();
	// The end of last segment after resize.
	pvMinSize += (lastSegment->getEndInExtents() + 1) * extentSize - (lvSize.get() - lvResultSize);

	// Hack: Due to metadata expectations, minimum size may be higher than current.
	Expected<quint64> pvSize = getSize();
	if (!pvSize.isOk())
		return pvSize;
	return qMin(pvMinSize, (quint64)pvSize.get());
}

Expected<void> Physical::resize(quint64 newSize) const
{
	Expected<quint64> pvSize = getSize();
	if (!pvSize.isOk())
		return pvSize;
	Logger::info(QString("Resizing PV %1 from %2 to %3")
	             .arg(m_partition).arg(pvSize.get()).arg(newSize));
	if (pvSize.get() == newSize)
		return Expected<void>();

	const Lvm::Group& group = m_physical.getGroup();
	if (!group.isResizeable() || !group.isWriteable())
	{
		return Expected<void>::fromMessage(
				QString("VG %1 is not modifiable").arg(group.getName()));
	}

	boost::optional<Lvm::Segment> lastSegment = m_physical.getLastSegment();
	if (!lastSegment)
	{
		// Resize empty PV.
		return pvresize(newSize);
	}

	Mode::mode_type mode = Mode::get(newSize, pvSize.get());

	Expected<qint64> lvDeltaRes = boost::apply_visitor(
			Visitor::LVDelta(*this, newSize, *lastSegment), mode);
	if (!lvDeltaRes.isOk())
		return lvDeltaRes;

	// TODO: Check. The calculated "current" size is different from real.
	qint64 lvDelta = lvDeltaRes.get();
	if (lvDelta == 0)
	{
		// We do not need/cannot resize LV.
		return pvresize(newSize);
	}

	QString lvName = Logical::getName(group, *lastSegment);
	Logical logical(m_g, lvName, m_gfsAction);
	return boost::apply_visitor(
			Visitor::ResizePV(*this, newSize, lvDelta, logical), mode);
}

} // namespace Volume

namespace VG
{

////////////////////////////////////////////////////////////
// Controller

Expected<QStringList> Controller::get() const
{
	int ret;
	if ((ret = guestfs_vgscan(m_g)))
		return Expected<QStringList>::fromMessage("Unable to scan VGs", ret);
	Expected<void> res = activate();
	if (!res.isOk())
		return res;
	char **vgs = guestfs_vgs(m_g);
	if (vgs == NULL)
		return Expected<QStringList>::fromMessage("Unable to get VG list", ret);

	QStringList result;
	for (char **cur = vgs; *cur != NULL; ++cur)
	{
		result << *cur;
		free(*cur);
	}
	free(vgs);
	return result;
}

Expected<Lvm::Config> Controller::getConfig(const QString &vg) const
{
	size_t size;
	char *ret;
	if ((ret = guestfs_vgmeta(m_g, QSTR2UTF8(vg), &size)) == NULL)
	{
		return Expected<Lvm::Config>::fromMessage(
				QString("Unable to get metadata for VG '%1'").arg(vg));
	}

	QString config = QByteArray(ret, size);
	free(ret);
	return Lvm::Config::create(config, vg);
}

Expected<void> Controller::activate() const
{
	int ret;
	Logger::info("vg_activate_all 1");
	if ((ret = guestfs_vg_activate_all(m_g, 1)))
		return Expected<void>::fromMessage("Unable to activate VGs");
	return Expected<void>();
}

Expected<void> Controller::deactivate() const
{
	int ret;
	Logger::info("vg_activate_all 0");
	if ((ret = guestfs_vg_activate_all(m_g, 0)))
		return Expected<void>::fromMessage("Unable to deactivate VGs");
	return Expected<void>();
}

Expected<quint64> Controller::getTotalFree() const
{
	struct guestfs_lvm_vg_list *vgs = guestfs_vgs_full(m_g);
	if (vgs == NULL)
		return Expected<quint64>::fromMessage("Unable to get VG stats");

	quint64 free = 0;
	for (uint32_t i = 0; i < vgs->len; ++i)
		free += vgs->val[i].vg_free;

	guestfs_free_lvm_vg_list(vgs);
	return free;
}

} // namespace VG

////////////////////////////////////////////////////////////
// Helper

Expected<QString> Helper::getPartitionTable() const
{
	char *partTable = guestfs_part_get_parttype(m_g, GUESTFS_DEVICE);
	if (!partTable)
	{
		int err = guestfs_last_errno(m_g);
		// This should catch no-partition-table cases.
		if (err == EINVAL)
		{
			return Expected<QString>::fromMessage(
					"No partition table", ERR_NO_PARTITION_TABLE);
		}
		return Expected<QString>::fromMessage("Unable to get partition table type");
	}

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

Expected<quint64> Helper::getSectorSize() const
{
	qint64 ret = guestfs_blockdev_getss(m_g, GUESTFS_DEVICE);
	if (ret < 0)
		return Expected<quint64>::fromMessage("Unable to get sector size");
	return ret;
}

Expected<quint64> Helper::getSize64(const QString &device) const
{
	qint64 ret = guestfs_blockdev_getsize64(m_g, QSTR2UTF8(device));
	if (ret < 0)
	{
		return Expected<quint64>::fromMessage(
				QString("Unable to get size of %1").arg(device));
	}
	return ret;
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
	Expected<QList<Partition::Unit> > parts = m_partList->get();
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
	Expected<Partition::Unit> firstPart = m_partList->getFirst();
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
	Expected<int> partCount = m_partList->getCount();
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

	int ret = guestfs_part_expand_gpt(m_g.get(), GUESTFS_DEVICE);
	if (ret < 0)
		return Expected<void>::fromMessage("Unable to move GPT backup header");
	return Expected<void>();
}

Expected<Wrapper::partMap_type> Wrapper::getLogical() const
{
	Expected<QList<Partition::Unit> > parts = m_partList->get();
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
		Partition::Unit part(m_g.get(), m_gfsAction,
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

Expected<void> Wrapper::sync() const
{
	int ret;
	Logger::info("sync");
	if ((ret = guestfs_sync(m_g.get())))
		return Expected<void>::fromMessage("Unable to sync image");
	return Expected<void>();
}

////////////////////////////////////////////////////////////
// Map

Expected<Wrapper> Map::getWritable(const QString &path)
{
	if (m_token && m_token->isCancellationRequested())
		return Expected<void>::fromMessage("Operation was cancelled");

	QMap<QString, Wrapper>::iterator it = m_gfsMap.find(path);

	if (it != m_gfsMap.end() && it.value().isReadOnly())
	{
		// call destructor to avoid concurrency
		m_gfsMap.erase(it);
		it = m_gfsMap.end();
	}

	if (it == m_gfsMap.end())
	{
		// create rw
		Expected<Wrapper> gfs = Wrapper::create(path, m_gfsAction);
		if (!gfs.isOk())
			return gfs;

		if (m_token && m_token->isCancellationRequested())
			return Expected<void>::fromMessage("Operation was cancelled");

		it = m_gfsMap.insert(path, gfs.get());
	}

	return it.value();
}

Expected<Wrapper> Map::getReadonly(const QString &path)
{
	if (m_token && m_token->isCancellationRequested())
		return Expected<void>::fromMessage("Operation was cancelled");

	QMap<QString, Wrapper>::iterator it = m_gfsMap.find(path);

	if (it == m_gfsMap.end())
	{
		// create ro
		Expected<Wrapper> gfs = Wrapper::createReadOnly(path, m_gfsAction);
		if (!gfs.isOk())
			return gfs;

		if (m_token && m_token->isCancellationRequested())
			return Expected<void>::fromMessage("Operation was cancelled");

		it = m_gfsMap.insert(path, gfs.get());
	}

	return it.value();
}

} // namespace GuestFS
