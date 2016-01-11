///////////////////////////////////////////////////////////////////////////////
///
/// @file CommandVm_p.h
///
/// Command execution for VM disk images.
///
/// @author dandreev
///
/// Copyright (c) 2005-2016 Parallels IP Holdings GmbH
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
#ifndef COMMANDVM_P_H
#define COMMANDVM_P_H

#include "Command.h"
#include "GuestFSWrapper.h"
#include "Util.h"
#include "Errors.h"

namespace Command
{

////////////////////////////////////////////////////////////
// ResizeData

struct ResizeData
{
	quint64 m_currentSize;
	quint64 m_minSize;
	quint64 m_minSizeKeepFS;
	QString m_lastPartition;
	bool m_fsSupported;
	bool m_partitionSupported;

	ResizeData(quint64 currentSize):
		m_currentSize(currentSize), m_minSize(currentSize),
		m_minSizeKeepFS(currentSize), m_fsSupported(true),
		m_partitionSupported(true)
	{
	}

	void print(const SizeUnitType &unitType) const;
};

////////////////////////////////////////////////////////////
// ResizeHelper

struct ResizeHelper
{
	ResizeHelper(const Image::Info &image,
		const GuestFS::Map &gfsMap,
		const boost::optional<Call> &call = boost::optional<Call>())
	: m_image(image), m_adapter(call), m_gfsMap(gfsMap), m_call(call)
	{
	}

	Expected<Partition::Unit> getLastPartition();
	Expected<ResizeData> getResizeData();
	Expected<QString> createTmpImage(quint64 mb, const QString &backingFile = QString()) const;
	Expected<void> shrinkFSIfNeeded(quint64 mb);
	Expected<quint64> getNewFSSize(quint64 mb, const Partition::Unit &lastPartition);
	Expected<void> expandToFit(quint64 mb, const GuestFS::Wrapper &gfs);
	Expected<void> mergeIntoPrevious(const QString &path);
	Expected<GuestFS::Wrapper> getGFSWritable(const QString &path = QString());
	Expected<GuestFS::Wrapper> getGFSReadonly();


	const boost::optional<Call>& getCall() const
	{
		return m_call;
	}
	const Image::Info& getImage() const
	{
		return m_image;
	}

private:
	Expected<Partition::Stats> expandPartition(
	        const Partition::Unit &partition, quint64 mb,
	        const QString &partTable, const GuestFS::Wrapper &gfs);
	Expected<Partition::Stats> calculateNewPartition(
			quint64 mb, const Partition::Stats &stats,
			quint64 sectorSize, const QString &partTable);
	Expected<qint64> calculateFSDelta(quint64 mb, const Partition::Unit &lastPartition);

private:
	const Image::Info &m_image;
	CallAdapter m_adapter;
	GuestFS::Map m_gfsMap;
	boost::optional<Call> m_call;
};

namespace Resizer
{
namespace Ignore
{

////////////////////////////////////////////////////////////
// Shrink

struct Shrink
{
	Expected<void> execute(ResizeHelper& helper,  quint64 sizeMb) const;

	Expected<void> checkSpace(const Image::Info &image) const;
};

////////////////////////////////////////////////////////////
// Expand

struct Expand
{
	Expected<void> execute(ResizeHelper& helper, quint64 sizeMb) const;

	Expected<void> checkSpace(const Image::Info &image, quint64 sizeMb) const;
};

} // namespace Ignore

namespace Consider
{

////////////////////////////////////////////////////////////
// Shrink

struct Shrink
{
	Expected<void> execute(ResizeHelper& helper, quint64 sizeMb) const;

	Expected<void> checkSpace(const Image::Info &image) const;
};

////////////////////////////////////////////////////////////
// Expand

struct Expand
{
	Expected<void> execute(ResizeHelper& helper, quint64 sizeMb) const;

	Expected<void> checkSpace(const Image::Info &image, quint64 sizeMb) const;
};

} // namespace Consider

////////////////////////////////////////////////////////////
// Gpt

template <class T>
struct Gpt
{
	Gpt(const T &mode):
		m_mode(mode)
	{
	}

	Expected<void> execute(ResizeHelper& helper, quint64 sizeMb) const;

	const T& getMode() const
	{
		return m_mode;
	}

private:
	T m_mode;
};

typedef boost::variant<
	Ignore::Shrink,
	Ignore::Expand,
	Gpt<Ignore::Expand>,
	Consider::Shrink,
	Consider::Expand
	> mode_type;

} // namespace Resizer
} // namespace Command
#endif // COMMANDVM_P_H
