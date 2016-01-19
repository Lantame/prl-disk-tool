///////////////////////////////////////////////////////////////////////////////
///
/// @file Command.h
///
/// Command line options parsing and
/// command execution.
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
#ifndef COMMAND_H
#define COMMAND_H

#include <boost/program_options.hpp>
#include <boost/variant.hpp>

#include "ProgramOptions.h"
#include "Util.h"
#include "Expected.h"
#include "GuestFSWrapper.h"
#include "ImageInfo.h"
#include "Abort.h"

namespace Command
{

////////////////////////////////////////////////////////////
// DiskAware

struct DiskAware
{
	const QString& getDiskPath() const
	{
		return m_diskPath;
	}

	explicit DiskAware(const QString &diskPath):
		m_diskPath(diskPath)
	{
	}

private:
	QString m_diskPath;
};

////////////////////////////////////////////////////////////
// Default

struct Default: DiskAware
{
	Expected<void> executePloop() const;

protected:
	explicit Default(const DiskAware &disk):
		DiskAware(disk)
	{
	}
};

////////////////////////////////////////////////////////////
// Resize

struct Resize: DiskAware
{
	Resize(const DiskAware &disk, quint64 sizeMb,
		   bool resizeLastPartition, bool force,
		   const GuestFS::Map &gfsMap,
		   const boost::optional<Call> &call):
		DiskAware(disk), m_sizeMb(sizeMb),
		m_resizeLastPartition(resizeLastPartition), m_force(force),
		m_gfsMap(gfsMap), m_call(call)
	{
	}

	Expected<void> execute() const;
	Expected<void> executePloop() const;

private:
	quint64 m_sizeMb;
	bool m_resizeLastPartition;
	bool m_force;

	GuestFS::Map m_gfsMap;
	boost::optional<Call> m_call;
};

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
// ResizeInfo

struct ResizeInfo: Default
{
	ResizeInfo(const DiskAware &disk, const SizeUnitType &unitType):
		Default(disk), m_unitType(unitType)
	{
	}

	Expected<void> execute() const;
	Expected<void> executePloop() const;

private:
	SizeUnitType m_unitType;
};

////////////////////////////////////////////////////////////
// Compact

struct Compact: DiskAware
{
	Compact(const DiskAware &disk, bool force,
			const boost::optional<Call> &call):
		DiskAware(disk),  m_force(force),
		m_call(call)
	{
	}

	Expected<void> execute() const;
	Expected<void> executePloop() const;

private:
	bool m_force;

	boost::optional<Call> m_call;
};

////////////////////////////////////////////////////////////
// CompactInfo

struct CompactInfo: Default
{
	explicit CompactInfo(const DiskAware &disk):
		Default(disk)
	{
	}

	Expected<void> execute() const;
};

namespace Merge
{
namespace External
{

////////////////////////////////////////////////////////////
// Direct

struct Direct
{
	explicit Direct(const boost::optional<Call> &call):
		m_adapter(call)
	{
	}

	quint64 getNeededSpace(const Image::Chain &snapshotChain) const;
	Expected<void> doCommit(const QList<Image::Info> &chain) const;

private:
	CallAdapter m_adapter;
};

////////////////////////////////////////////////////////////
// Sequential

struct Sequential
{
	explicit Sequential(const boost::optional<Call> &call):
		m_adapter(call)
	{
	}

	quint64 getNeededSpace(const Image::Chain &snapshotChain) const;
	Expected<void> doCommit(const QList<Image::Info> &chain) const;

private:
	CallAdapter m_adapter;
};

typedef boost::variant<Direct, Sequential> mode_type;

////////////////////////////////////////////////////////////
// Executor

struct Executor: DiskAware
{
	Executor(const DiskAware &disk,
			 const mode_type &mode,
			 const boost::optional<Call> &call):
		DiskAware(disk), m_mode(mode), m_adapter(call)
	{
	}

	Expected<void> execute() const;
	/* Merges only the given subchain.
	 * Does not take disk lock.
	 * Does not use constructor 'disk' parameter.
	 */
	Expected<void> execute(const Image::Chain &snapshotChain) const;

private:
	mode_type m_mode;
	CallAdapter m_adapter;
};

} // namespace External

////////////////////////////////////////////////////////////
// Internal

struct Internal: DiskAware
{
	Internal(const DiskAware &disk, const boost::optional<Call> &call):
		DiskAware(disk), m_adapter(call)
	{
	}

	Expected<void> execute() const;

private:
	CallAdapter m_adapter;
};

typedef boost::variant<External::Executor, Internal> mode_type;

} // namespace Merge

////////////////////////////////////////////////////////////
// MergeSnapshots

struct MergeSnapshots: DiskAware
{
	MergeSnapshots(
			const DiskAware &disk,
			const Merge::mode_type &executor,
			const boost::optional<Call> &call):
		DiskAware(disk), m_executor(executor), m_call(call)
	{
	}

	// Check whether '-b' option (commit to specified base) is supported.
	static Expected<Merge::External::mode_type> getExternalMode(
			const boost::optional<Call> &m_call);

	Expected<void> execute() const;
	Expected<void> executePloop() const;

private:
	Merge::mode_type m_executor;
	boost::optional<Call> m_call;
};

////////////////////////////////////////////////////////////
// Traits

template <typename T>
struct Traits
{
	static boost::program_options::options_description getOptions();

	static const char m_action[];
	static const bool m_info;
};

////////////////////////////////////////////////////////////
// Factory

template <typename T>
struct Factory
{
	static Expected<Factory<T> > create(
			const std::vector<std::string> &args, const boost::optional<Call> &call,
			const GuestFS::Map &gfsMap);

	Expected<T> operator()() const;

private:
	Factory(const boost::program_options::variables_map &vm,
			const boost::optional<Call> &call,
			const GuestFS::Map &gfsMap):
		m_vm(vm), m_call(call), m_gfsMap(gfsMap)
	{
	}

private:
	boost::program_options::variables_map m_vm;

	boost::optional<Call> m_call;
	GuestFS::Map m_gfsMap;
};

} // namespace Command

////////////////////////////////////////////////////////////
// Visitor

struct Visitor
{
	static Expected<Visitor> create(const ParsedCommand &command);

	template <typename T>
	void operator()(const Command::Traits<T> &desc)
	{
		if (m_action == desc.m_action &&
			m_info == desc.m_info)
		{
			m_result = createAndExecute<T>();
		}
	}

	const Expected<void>& getResult() const
	{
		return m_result;
	}

private:
	Visitor(const ParsedCommand &command,
			const boost::program_options::variables_map &vm,
			const std::vector<std::string> &args);

	template <typename T>
	Expected<void> createAndExecute() const;

private:
	QString m_action;
	bool m_info;
	std::vector<std::string> m_args;
	Expected<void> m_result;
	GuestFS::Map m_gfsMap;

	boost::optional<Call> m_call;
	boost::optional<GuestFS::Action> m_gfsAction;
	Abort::token_type m_token;
};

////////////////////////////////////////////////////////////
// UsageVisitor

struct UsageVisitor
{
	UsageVisitor();

	template <typename T>
	void operator()(const Command::Traits<T> &desc)
	{
		m_result.add(desc.getOptions());
	}

	const boost::program_options::options_description& getResult() const
	{
		return m_result;
	}

private:
	boost::program_options::options_description m_result;
};

#endif
