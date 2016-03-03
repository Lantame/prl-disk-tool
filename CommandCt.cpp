///////////////////////////////////////////////////////////////////////////////
///
/// @file CommandCt.cpp
///
/// Command execution for container disk images (ploop).
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
#include <QFileInfo>
#include <boost/scope_exit.hpp>

#include "Command.h"
#include "StringTable.h"
#include "Util.h"
#include "Errors.h"

using namespace Command;

namespace
{

char PLOOP[] = "/usr/sbin/ploop";
char RESIZE2FS[] = "/usr/sbin/resize2fs";

enum {PLOOP_SECTOR_SIZE = 512,
	  PLOOP_OVERHEAD_BLOCKS = 4,
	  PLOOP_FS_BLOCK_SIZE = 4096};

QString getDescriptor(const QString &path)
{
	return QString("%1/%2").arg(path, DESCRIPTOR);
}

QString getPartition(const QString &device)
{
	return device + "p1";
}

////////////////////////////////////////////////////////////
// Ploop

struct Ploop
{
	struct Info
	{
		quint64 size;
		QString device;
		quint64 blockSize;
	};

	explicit Ploop(const CallAdapter &adapter):
		m_adapter(adapter)
	{
	}

	Expected<Info> getInfo(const QString &descriptor) const
	{
		QByteArray out;

		QStringList args = QStringList() << "info" << "-d" << "-s" << descriptor;
		int ret = m_adapter.run(PLOOP, args, &out);
		if (ret)
		{
			return Expected<Info>::fromMessage(
				QString(IDS_ERR_SUBPROGRAM_RETURN_CODE)
				.arg(PLOOP, args.join(" ")).arg(ret));
		}

		QStringList lines = QString(out).split('\n');
		Info result;

		QRegExp sizeRE("^size:\\s+(\\d+)");
		if (lines.indexOf(sizeRE) == -1)
			return Expected<Info>::fromMessage("Cannot get ploop device size");
		result.size = sizeRE.cap(1).toULongLong() * PLOOP_SECTOR_SIZE;

		QRegExp deviceRE("^device:\\s+(\\S+)");
		if (lines.indexOf(deviceRE) == -1)
		{
			return Expected<Info>::fromMessage(
					"Cannot get ploop device", ERR_PLOOP_NOT_MOUNTED);
		}
		result.device = deviceRE.cap(1);

		QRegExp blockRE("^blocksize:\\s+(\\d+)");
		if (lines.indexOf(blockRE) == -1)
			return Expected<Info>::fromMessage("Cannot get ploop block size");
		result.blockSize = blockRE.cap(1).toULongLong() * PLOOP_SECTOR_SIZE;

		return result;
	}

	Expected<void> mount(const QString &descriptor) const
	{
		QStringList args = QStringList() << "mount" << descriptor;
		int ret = m_adapter.run(PLOOP, args);
		if (ret)
		{
			return Expected<void>::fromMessage(
					QString("Cannot mount descriptor: %1").arg(descriptor));
		}
		return Expected<void>();
	}

	Expected<void> umount(const QString &descriptor) const
	{
		QStringList args = QStringList() << "umount" << descriptor;
		int ret = m_adapter.run(PLOOP, args);
		if (ret)
		{
			return Expected<void>::fromMessage(
					QString("Cannot unmount descriptor: %1").arg(descriptor));
		}
		return Expected<void>();
	}

	Expected<quint64> getMinSizeBlocks(const QString &partition) const
	{
		QByteArray out;
		QStringList args = QStringList() << "-P" << "-f" << partition;
		int ret = m_adapter.run(RESIZE2FS, args, &out);
		if (ret)
		{
			return Expected<quint64>::fromMessage(
				QString(IDS_ERR_SUBPROGRAM_RETURN_CODE)
				.arg(RESIZE2FS, args.join(" ")).arg(ret));
		}
		QStringList lines = QString(out).split('\n');

		QRegExp minSizeRE("^Estimated minimum size of the filesystem: (\\d+)");
		if (lines.indexOf(minSizeRE) == -1)
			return Expected<quint64>::fromMessage(IDS_ERR_CANNOT_PARSE_MIN_SIZE);
		return minSizeRE.cap(1).toULongLong();
	}

private:
	CallAdapter m_adapter;
};

////////////////////////////////////////////////////////////
// Mounted

struct Mounted
{
	Mounted(const Ploop &ploop, const Ploop::Info &info):
		m_ploop(ploop), m_info(info)
	{
	}

	Expected<ResizeData> getResizeData() const
	{
		QString partition = getPartition(m_info.device);
		Expected<quint64> minBlocks = m_ploop.getMinSizeBlocks(partition);
		if (!minBlocks.isOk())
			return minBlocks;

		ResizeData data(m_info.size);
		data.m_minSize = PLOOP_OVERHEAD_BLOCKS * m_info.blockSize +
						 minBlocks.get() * PLOOP_FS_BLOCK_SIZE;
		data.m_lastPartition = "/dev/sda1";
		return data;
	}

private:
	Ploop m_ploop;
	Ploop::Info m_info;
};

////////////////////////////////////////////////////////////
// Unmounted

struct Unmounted
{
	Unmounted(const Ploop &ploop, const QString &descriptor):
		m_ploop(ploop), m_descriptor(descriptor)
	{
	}

	Expected<ResizeData> getResizeData() const
	{
		Expected<void> res = m_ploop.mount(m_descriptor);
		if (!res.isOk())
			return res;

		BOOST_SCOPE_EXIT(&m_ploop, &m_descriptor)
		{
			m_ploop.umount(m_descriptor);
		} BOOST_SCOPE_EXIT_END

		Expected<Ploop::Info> info = m_ploop.getInfo(m_descriptor);
		if (!info.isOk())
			return info;

		return Mounted(m_ploop, info.get()).getResizeData();
	}

private:
	Ploop m_ploop;
	QString m_descriptor;
};

typedef boost::variant<Mounted, Unmounted> mounted_type;

Expected<mounted_type> getMountState(const QString &descriptor)
{
	CallAdapter adapter = CallAdapter(Call());
	Ploop ploop(adapter);

	Expected<Ploop::Info> info = ploop.getInfo(descriptor);
	if (!info.isOk())
	{
		if (info.getCode() != ERR_PLOOP_NOT_MOUNTED)
			return Expected<void>(info);
		return mounted_type(Unmounted(ploop, descriptor));
	}
	return mounted_type(Mounted(ploop, info.get()));
}

namespace Visitor
{

////////////////////////////////////////////////////////////
// ResizeInfo

struct ResizeInfo: boost::static_visitor<Expected<ResizeData> >
{
	template <class T>
	Expected<ResizeData> operator() (const T &mode) const
	{
		return mode.getResizeData();
	}
};

} // namespace Visitor

} // namespace

Expected<void> Default::executePloop() const
{
	return Expected<void>::fromMessage("This action is not implemented for ploop");
}

Expected<void> Resize::executePloop() const
{
	QByteArray size = QString("%1M").arg(m_sizeMb).toUtf8();
	QByteArray path = getDescriptor(getDiskPath()).toUtf8();
	char p1[] = "resize", p2[] = "-s";
	char *args[] = {PLOOP, p1, p2, size.data(), path.data(), NULL};
	CallAdapter(m_call).execvp(PLOOP, args);
	return Expected<void>::fromMessage(IDS_ERR_PLOOP_EXEC_FAILED);
}

Expected<void> ResizeInfo::executePloop() const
{
	QString descriptor = getDescriptor(getDiskPath());
	Expected<mounted_type> mode = getMountState(descriptor);
	if (!mode.isOk())
		return mode;

	Expected<ResizeData> data = boost::apply_visitor(
			Visitor::ResizeInfo(), mode.get());
	if (!data.isOk())
		return data;

	data.get().print(m_unitType);
	return Expected<void>();
}

Expected<void> Compact::executePloop() const
{
	QByteArray path = getDescriptor(getDiskPath()).toUtf8();
	char p1[] = "balloon", p2[] = "discard", p3[] = "--automount", p4[] = "--defrag";
	char *args[] = {PLOOP, p1, p2, p3, p4, path.data(), NULL};
	CallAdapter(m_call).execvp(PLOOP, args);
	return Expected<void>::fromMessage(IDS_ERR_PLOOP_EXEC_FAILED);
}

Expected<void> MergeSnapshots::executePloop() const
{
	QByteArray path = getDescriptor(getDiskPath()).toUtf8();
	char p1[] = "snapshot-merge", p2[] = "-A";
	char *args[] = {PLOOP, p1, p2, path.data(), NULL};
	CallAdapter(m_call).execvp(PLOOP, args);
	return Expected<void>::fromMessage(IDS_ERR_PLOOP_EXEC_FAILED);
}
