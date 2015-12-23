///////////////////////////////////////////////////////////////////////////////
///
/// @file GuestFSWrapper.h
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
#ifndef GUESTFS_WRAPPER_H
#define GUESTFS_WRAPPER_H

#include <sys/statvfs.h>
#include <algorithm>

#include <boost/shared_ptr.hpp>
#include <boost/variant.hpp>

#include <QString>
#include <QStringList>
#include <QPair>
#include <QMap>

#include <guestfs.h>

#include "Util.h"
#include "Expected.h"
#include "Abort.h"
#include "Lvm.h"

namespace GuestFS
{
namespace VG
{

////////////////////////////////////////////////////////////
// Controller

struct Controller
{
	explicit Controller(guestfs_h *g):
		m_g(g)
	{
	}

	Expected<QStringList> get() const;
	Expected<Lvm::Config> getConfig(const QString &name) const;

	Expected<void> activate() const;
	Expected<void> deactivate() const;

private:
	guestfs_h *m_g;
};

} // namespace VG

////////////////////////////////////////////////////////////
// Helper

struct Helper
{
	explicit Helper(guestfs_h *g):
		m_g(g), m_vg(g)
	{
	}

	/* 'msdos' or 'gpt' */
	Expected<QString> getPartitionTable() const;
	Expected<struct statvfs> getFilesystemStats(const QString &name) const;

	const VG::Controller& getVG() const
	{
		return m_vg;
	}

	Expected<quint64> getSectorSize() const;
	Expected<quint64> getSize64(const QString &device) const;

private:
	guestfs_h *m_g;
	VG::Controller m_vg;
};

////////////////////////////////////////////////////////////
// Action

struct Action
{
	template <typename FS>
	static FS get(guestfs_h *g, const QString &partition)
	{
		return FS(g, partition);
	}
};

////////////////////////////////////////////////////////////
// Ext

struct Ext
{
	Ext()
	{
	}

	Ext(guestfs_h *g, const QString &partition):
		m_g(g), m_partition(partition)
	{
	}

	int resize(quint64 newSize) const;
	Expected<quint64> getMinSize() const;

private:
	guestfs_h *m_g;
	QString m_partition;
};

////////////////////////////////////////////////////////////
// Ntfs

struct Ntfs
{
	Ntfs()
	{
	}

	Ntfs(guestfs_h *g, const QString &partition):
		 m_g(g), m_partition(partition)
	{
	}

	int resize(quint64 newSize) const;
	Expected<quint64> getMinSize() const;

private:
	guestfs_h *m_g;
	QString m_partition;
};

////////////////////////////////////////////////////////////
// Btrfs

struct Btrfs
{
	Btrfs()
	{
	}

	Btrfs(guestfs_h *g, const QString &partition):
		  m_g(g), m_partition(partition)
	{
	}

	int resize(quint64 newSize) const;
	Expected<quint64> getMinSize() const;

private:
	guestfs_h *m_g;
	QString m_partition;
};

////////////////////////////////////////////////////////////
// Xfs

struct Xfs
{
	Xfs()
	{
	}

	Xfs(guestfs_h *g, const QString &partition):
		m_g(g), m_partition(partition)
	{
	}

	// Grow up to maximum possible.
	int resize() const;
	Expected<quint64> getMinSize() const;

private:
	guestfs_h *m_g;
	QString m_partition;
};

////////////////////////////////////////////////////////////
// Swap

struct Swap
{
	static quint64 getMinSize();
};

////////////////////////////////////////////////////////////
// Unknown

struct Unknown
{
};

namespace Volume
{

////////////////////////////////////////////////////////////
// Logical

struct Logical
{
	Logical(guestfs_h *g, const QString &fullName,
			const boost::optional<Action> &gfsAction):
		m_g(g), m_fullName(fullName), m_gfsAction(gfsAction)
	{
	}

	static QString getName(const Lvm::Group &group, const Lvm::Segment &segment);

	Expected<quint64> getSize() const;
	Expected<quint64> getMinSize() const;

private:
	guestfs_h *m_g;
	QString m_fullName;
	boost::optional<Action> m_gfsAction;
};

////////////////////////////////////////////////////////////
// Physical

struct Physical
{
	explicit Physical(const Lvm::Physical &physical):
		m_physical(physical)
	{
	}

	Physical(const Lvm::Physical &physical, guestfs_h *g, const QString &partition,
	         const boost::optional<Action> &gfsAction):
		m_physical(physical), m_g(g), m_partition(partition),
		m_gfsAction(gfsAction)
	{
	}

	const Lvm::Physical& getPhysical() const
	{
		return m_physical;
	}

	Expected<quint64> getMinSize() const;

private:
	Lvm::Physical m_physical;
	guestfs_h *m_g;
	QString m_partition;
	boost::optional<Action> m_gfsAction;
};

} // namespace Volume

typedef boost::variant<
	Unknown,
	Ext,
	Ntfs,
	Btrfs,
	Xfs,
	Swap,
	Volume::Physical
> fs_type;

namespace Partition
{

////////////////////////////////////////////////////////////
// Stats

struct Stats
{
	quint64 start;
	quint64 end;
	quint64 size;
};

namespace Attribute
{

////////////////////////////////////////////////////////////
// Gpt

struct Gpt
{
	Gpt(const QString &name, const QString &gptType, const QString &gptGuid):
		m_name(name), m_gptType(gptType), m_gptGuid(gptGuid)
	{
	}

	QString m_name;
	QString m_gptType;
	QString m_gptGuid;
};

////////////////////////////////////////////////////////////
// Mbr

struct Mbr
{
	Mbr(int mbrId, const QString &gptType = QString()):
		m_mbrId(mbrId), m_gptType(gptType)
	{
	}

	bool isExtended() const
	{
		// Most common, according to wiki.
		int containerIds[16] = {0x5, 0xF, 0x15, 0x1F, 0x42, 0x82, 0x85, 0x91, 0x9B, 0xA5, 0xA6, 0xA9, 0xBF, 0xC5, 0xCF, 0xD5};
		return std::find(containerIds, containerIds + 16, m_mbrId) != containerIds + 16;
	}

	int m_mbrId;
	QString m_gptType;
};

////////////////////////////////////////////////////////////
// Aggregate

struct Aggregate
{
	Aggregate(bool bootable, const Gpt &option):
		m_bootable(bootable), m_option(option)
	{
	}

	Aggregate(bool bootable, const Mbr &option):
		m_bootable(bootable), m_option(option)
	{
	}

	bool isBootable() const
	{
		return m_bootable;
	}

	bool isExtended() const
	{
		const Mbr* ptr = boost::get<Mbr>(&m_option);
		return ptr ? ptr->isExtended() : false;
	}

	template <class T>
	boost::optional<T> get() const
	{
		const T* ptr = boost::get<T>(&m_option);
		return boost::optional<T>(ptr, *ptr);
	}

private:
	bool m_bootable;
	boost::variant<Gpt, Mbr> m_option;
};

} // namespace Attribute

////////////////////////////////////////////////////////////
// Unit

struct Unit
{
	Unit(guestfs_h *g, const boost::optional<Action> &gfsAction,
		 const QString &name, const fs_type &filesystem = Unknown()):
		m_g(g),  m_helper(g), m_gfsAction(gfsAction),
		m_name(name), m_filesystem(filesystem)
	{
	}

	const QString& getName() const
	{
		return m_name;
	}

	int getIndex() const;

	Expected<bool> isLogical() const;
	Expected<bool> isExtended() const;

	Expected<Partition::Stats> getStats() const;

	Expected<quint64> getMinSize() const;

	template <class T> const T* getFilesystem() const;

	const GuestFS::fs_type& getFilesystem() const
	{
		return m_filesystem;
	}

	/* Disk-modifying */
	Expected<void> shrinkFilesystem(quint64 dec) const;

	/* Disk-modifying */
	Expected<void> resizeFilesystem(quint64 newSize) const;

	Expected<bool> isFilesystemSupported() const;

	Expected<struct statvfs> getFilesystemStats() const;

	Expected<Attribute::Aggregate> getAttributes() const;

	/* Disk-modifying */
	Expected<void> apply(const Attribute::Aggregate &attrs) const;

private:
	Expected<quint64> getMinSize(const fs_type &fs) const;

	template<class T> Expected<T> getAttributesInternal() const;

private:
	guestfs_h *m_g;
	Helper m_helper;
	boost::optional<Action> m_gfsAction;
	QString m_name;
	fs_type m_filesystem;
};

////////////////////////////////////////////////////////////
// List

struct List
{
	List(guestfs_h *g, const boost::optional<Action> &gfsAction):
		m_g(g), m_gfsAction(gfsAction)
	{
	}

	Expected<Unit> getFirst() const;
	Expected<Unit> getLast() const;
	Expected<int> getCount() const;
	Expected<QList<Unit> > get() const;

	Expected<Unit> createUnit(const QString &name) const;

private:
	Expected<void> load() const;
	Expected<QMap<QString, fs_type> > getFilesystems() const;
	Expected<QMap<QString, fs_type> > getContent() const;

	guestfs_h *m_g;
	boost::optional<Action> m_gfsAction;
	// Lazy-initialized cache.
	mutable boost::shared_ptr<QList<Unit> > m_partitions;
};

} // namespace Partition

////////////////////////////////////////////////////////////
// Wrapper

struct Wrapper
{
	static Expected<Wrapper> create(
			const QString &filename,
			const boost::optional<Action> &gfsAction = boost::optional<Action>());

	static Expected<Wrapper> createReadOnly(
			const QString &filename,
			const boost::optional<Action> &gfsAction = boost::optional<Action>());

	bool isReadOnly() const
	{
		return m_readOnly;
	}

	Expected<Partition::Unit> getLastPartition() const
	{
		return m_partList.getLast();
	}

	/* Return extended partition (at most 1 possible).
	 * Returns error if no extended partitions exist. */
	Expected<Partition::Unit> getContainer() const;

	/* virt-resize uses this overhead to prevent data corruption.
	 * Though real overhead may be less, virt-resize will refuse to resize
	 * if we do not have enough space. Here we calculate overhead as in
	 * @see libguestfs/resize/resize.ml
	 * We use it to resize filesystem on last partition more by this value. */
	Expected<quint64> getVirtResizeOverhead() const;

	Expected<QList<Partition::Unit> > getPartitions() const
	{
		return m_partList.get();
	}

	Expected<quint64> getBlockSize() const;

	/* 'msdos' or 'gpt' */
	Expected<QString> getPartitionTable() const
	{
		return m_helper.getPartitionTable();
	}

	/* Disk-modifying */
	Expected<void> expandGPT() const;

	/* Disk-modifying.
	 * Remove partition and create it with given start and end sectors.
	 * NB: In case of not-last partition, the partition naming may change.
	 * Preserves attributes (bootable, gpt type, mbrId, gpt name).
	 */
	Expected<void> resizePartition(const Partition::Unit &partition,
	                               quint64 startSector, quint64 endSector) const;

	Expected<quint64> getSectorSize() const
	{
		return m_helper.getSectorSize();
	}

private:
	struct HandleDestroyer
	{
		void operator ()(guestfs_h *g)
		{
			guestfs_shutdown(g);
			guestfs_close(g);
		}
	};

	typedef QPair<Partition::Stats, Partition::Attribute::Aggregate> partInfo_type;
	typedef QMap<int, partInfo_type> partMap_type;

	Wrapper(const boost::shared_ptr<guestfs_h> &g,
			const boost::optional<Action> &gfsAction,
			bool readOnly):
		m_g(g), m_gfsAction(gfsAction), m_helper(g.get()),
		m_partList(g.get(), m_gfsAction), m_readOnly(readOnly)
	{
	}

	Expected<partMap_type> getLogical() const;
	Expected<void> createLogical(const partMap_type &logical) const;

private:
	boost::shared_ptr<guestfs_h> m_g;
	boost::optional<Action> m_gfsAction;
	Helper m_helper;
	Partition::List m_partList;
	bool m_readOnly;
};

////////////////////////////////////////////////////////////
// Map

struct Map
{
	Map(const boost::optional<Action> action = boost::optional<Action>(),
		const Abort::token_type& token = Abort::token_type()):
		m_token(token), m_gfsAction(action)
	{
	}

	Expected<Wrapper> getWritable(const QString &path);
	Expected<Wrapper> getReadonly(const QString &path);


private:
	QMap<QString, GuestFS::Wrapper> m_gfsMap;
	Abort::token_type m_token;
	boost::optional<Action> m_gfsAction;
};

} // namespace GuestFS

#endif // GUESTFS_WRAPPER_H
