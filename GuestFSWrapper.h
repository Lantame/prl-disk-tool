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

#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>

#include <QString>
#include <QStringList>

#include <guestfs.h>

#include "Util.h"
#include "Expected.h"

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

////////////////////////////////////////////////////////////
// List

struct List
{
	explicit List(guestfs_h *g):
		m_g(g)
	{
	}

	Expected<QString> getFirst() const;
	Expected<QString> getLast() const;
	Expected<int> getCount() const;
	Expected<QStringList> get() const;

private:
	Expected<void> load() const;

	guestfs_h *m_g;
	// Lazy-initialized cache.
	mutable boost::shared_ptr<QStringList> m_partitions;
};

} // namespace Partition

namespace GuestFS
{

////////////////////////////////////////////////////////////
// Ext

struct Ext
{
	Ext(guestfs_h *g, const QString &partition):
		m_g(g), m_partition(partition)
	{
	}

	int resize(quint64 newSize) const;
	Expected<quint64> getMinSize(const struct statvfs &stats) const;

private:
	guestfs_h *m_g;
	QString m_partition;
};

////////////////////////////////////////////////////////////
// Ntfs

struct Ntfs
{
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

	Expected<QString> getLastPartition() const
	{
		return m_partList.getLast();
	}

	Expected<Partition::Stats>
		getPartitionStats(const QString &partition) const;

	/* virt-resize uses this overhead to prevent data corruption.
	 * Though real overhead may be less, virt-resize will refuse to resize
	 * if we do not have enough space. Here we calculate overhead as in
	 * @see libguestfs/resize/resize.ml
	 * We use it to resize filesystem on last partition more by this value. */
	Expected<quint64> getVirtResizeOverhead() const;

	Expected<quint64> getPartitionMinSize(const QString &partition) const;

	/* Disk-modifying */
	Expected<void> shrinkFilesystem(const QString &partition, quint64 dec) const;

	Expected<QStringList> getPartitions() const
	{
		return m_partList.get();
	}

	Expected<quint64> getBlockSize() const;

	Expected<struct statvfs> getFilesystemStats(const QString &partition) const;

private:
	struct HandleDestroyer
	{
		void operator ()(guestfs_h *g)
		{
			guestfs_shutdown(g);
			guestfs_close(g);
		}
	};

	Wrapper(const boost::shared_ptr<guestfs_h> &g,
			const boost::optional<Action> &gfsAction,
			bool readOnly):
		m_g(g), m_gfsAction(gfsAction), m_partList(g.get()), m_readOnly(readOnly)
	{
	}

	Expected<void> shrinkFilesystem(const QString &partition, quint64 newSize, const QString &fs) const;

	Expected<QString> getPartitionFS(const QString &partition) const;

	Expected<quint64> getPartitionMinSize(const QString &partition, const QString &fs) const;

	Expected<quint64> getSectorSize() const;

private:
	boost::shared_ptr<guestfs_h> m_g;
	boost::optional<Action> m_gfsAction;
	Partition::List m_partList;
	bool m_readOnly;
};

} // namespace GuestFS

#endif // GUESTFS_WRAPPER_H
