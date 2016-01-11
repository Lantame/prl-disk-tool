///////////////////////////////////////////////////////////////////////////////
///
/// @file Command.cpp
///
/// Command line options parsing.
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
#include <limits>

#include <QFileInfo>
#include <QDir>

#include "Command.h"
#include "Util.h"
#include "StringTable.h"

namespace po = boost::program_options;

using namespace Command;
using namespace GuestFS;

namespace
{

const char DESCRIPTOR[] = "DiskDescriptor.xml";

// Functions

/** Get size unit type by letter */
bool determineSizeUnitType(const QChar &letter, SizeUnitType &unitType)
{
	if (!letter.isLetter())
		return false;

	switch(letter.toAscii())
	{
		case 'K':
			unitType = SIZEUNIT_K;
			break;
		case 'M':
			unitType = SIZEUNIT_M;
			break;
		case 'G':
			unitType = SIZEUNIT_G;
			break;
		case 'T':
			unitType = SIZEUNIT_T;
			break;
		default:
			return false;
	}

	return true;
}

bool determineSizeUnitType(const QString &letter, SizeUnitType &unitType)
{
	if (letter.length() != 1)
		return false;
	return determineSizeUnitType(letter[0], unitType);
}

/** Convert any size to megabytes */
Expected<quint64> toSizeMb(quint64 size, SizeUnitType unitType)
{
	switch(unitType)
	{
		case SIZEUNIT_b:
			return size / 1024 / 1024;
		case SIZEUNIT_K:
			return size / 1024;
		case SIZEUNIT_M:
			return size;
		case SIZEUNIT_G:
			if (size > std::numeric_limits<quint64>::max() / 1024)
				return Expected<quint64>::fromMessage("Size too big");
			return size * 1024;
		case SIZEUNIT_T:
			if (size > std::numeric_limits<quint64>::max() / 1024 / 1024)
				return Expected<quint64>::fromMessage("Size too big");
			return size * 1024 * 1024;
		case SIZEUNIT_s:
			return size / (1024 / 512) / 1024;
		default:
			Q_ASSERT(0);
			return 0;
	}
}

Expected<quint64> parseSizeMb(const QString &value)
{
	QString str = value.trimmed();

	QChar lastChar = str[str.size()-1];
	if (lastChar==0)
		return Expected<quint64>::fromMessage("Non-ascii size");

	QString digits;
	SizeUnitType unitType;
	if (lastChar.isLetter())
	{
		if (!determineSizeUnitType(lastChar, unitType))
			return Expected<quint64>::fromMessage("Unknown size unit");
		digits = str.mid(0, str.size()-1);
	}
	else if (lastChar.isDigit())
	{
		unitType = SIZEUNIT_M;
		digits = str;
	}
	else
		return Expected<quint64>::fromMessage("Wrong character in size");

	bool ok;
	quint64 size = digits.toULongLong(&ok);

	if (!ok)
		return Expected<quint64>::fromMessage("Cannot parse size");

	return toSizeMb(size, unitType);
}

bool applyDiskPath(const QString &src, QString &dst)
{
	QFileInfo fileInfo;

	fileInfo.setFile(src);
	if (!fileInfo.exists())
		return false;
	dst = src;

	// For ct ploop, it may contain dir with DiskDescriptor.xml
	// or a file near DiskDescriptor.xml.
	// For vm image, a file only.
	if ((fileInfo.isFile() && !fileInfo.dir().exists(DESCRIPTOR)) || // vm image file
		(fileInfo.isDir() && QDir(fileInfo.filePath()).exists(DESCRIPTOR))) // ct image dir
		dst = fileInfo.absoluteFilePath();
	else if (fileInfo.isFile() && fileInfo.dir().exists(DESCRIPTOR)) // file in ct image dir
		dst = fileInfo.dir().absolutePath();
	else
		return false;
	if (dst.endsWith("/"))
		dst = dst.left(dst.length()-1);

	return true;
}

bool isPloop(const QString &path)
{
	if (path.isEmpty())
		return false;

	/* FIXME: check by CT layout, maybe some hint in the DiskDescriptor.xml */
	if (QFileInfo(path).exists() &&
			QFileInfo(QString("%1/%2").arg(path, DESCRIPTOR)).exists())
		return true;

	return false;
}

} // namespace

namespace Command
{

////////////////////////////////////////////////////////////
// Traits

template<> const char Traits<Resize>::m_action[] = "resize";
template<> const bool Traits<Resize>::m_info = false;

template<> const char Traits<ResizeInfo>::m_action[] = "resize";
template<> const bool Traits<ResizeInfo>::m_info = true;

template<> const char Traits<Compact>::m_action[] = "compact";
template<> const bool Traits<Compact>::m_info = false;

template<> const char Traits<CompactInfo>::m_action[] = "compact";
template<> const bool Traits<CompactInfo>::m_info = true;

template<> const char Traits<MergeSnapshots>::m_action[] = "merge";
template<> const bool Traits<MergeSnapshots>::m_info = false;

template<> const char Traits<Convert>::m_action[] = "convert";
template<> const bool Traits<Convert>::m_info = false;

template<> po::options_description Traits<Resize>::getOptions()
{
	po::options_description options("Disk resizing (\"resize\")");
	options.add_options()
		("size", po::value<std::string>(), "Set the virtual hard disk size")
		("force", "Forcibly drop the suspended state")
		("resize_partition", "Resize last partition and its filesystem")
		("hdd", po::value<std::string>(), "Full path to the disk")
		;
	return options;
}

template<> po::options_description Traits<ResizeInfo>::getOptions()
{
	po::options_description options("Disk resizing estimates (\"resize --info|-i\")");
	options.add_options()
		("units", po::value<std::string>(), "Units to display disk size (K|M|G)")
		("hdd", po::value<std::string>(), "Full path to the disk")
		;
	return options;
}

template<> po::options_description Traits<Compact>::getOptions()
{
	po::options_description options("Disk compacting (\"compact\")");
	options.add_options()
		("force", "Forcibly drop the suspended state")
		("hdd", po::value<std::string>(), "Full path to the disk")
		;
	return options;
}

template<> po::options_description Traits<CompactInfo>::getOptions()
{
	po::options_description options("Disk compacting estimates (\"compact --info|-i\")");
	options.add_options()
		("hdd", po::value<std::string>(), "Full path to the disk")
		;
	return options;
}

template<> po::options_description Traits<MergeSnapshots>::getOptions()
{
	po::options_description options("Disk snapshots merge (\"merge\")");
	options.add_options()
		("external", "Merge external snapshots (default: internal)")
		("hdd", po::value<std::string>(), "Full path to the disk")
		;
	return options;
}

template<> po::options_description Traits<Convert>::getOptions()
{
	po::options_description options("Disk conversion (\"convert\")");
	options.add_options()
		("expanding", "Convert disk to expanding (increasing capacity)")
		("plain", "Convert disk to plain (fixed capacity)")
		("hdd", po::value<std::string>(), "Full path to the disk")
		;
	return options;
}

////////////////////////////////////////////////////////////
// Factory

template <>
struct Factory<DiskAware>
{
	static Expected<DiskAware> build(const po::variables_map &vm);
};

Expected<DiskAware> Factory<DiskAware>::build(const po::variables_map &vm)
{
	po::variables_map::const_iterator argIter;
	QString diskPath;
	if ((argIter = vm.find(OPT_DISKPATH)) == vm.end() ||
		!applyDiskPath(QString::fromStdString(argIter->second.as<std::string>()), diskPath))
		return Expected<DiskAware>::fromMessage(IDS_ERR_INVALID_HDD);
	return DiskAware(diskPath);
}

template <typename T>
Expected<Factory<T> > Factory<T>::create(
		const std::vector<std::string> &args,
		const boost::optional<Call> &call,
		const GuestFS::Map &gfsMap)
{
	po::variables_map vm;
	try
	{
		po::store(po::command_line_parser(args)
			.options(Traits<T>::getOptions())
			.run(), vm);
	}
	catch (po::error &e)
	{
		return Expected<Factory<T> >::fromMessage(e.what());
	}
	return Factory(vm, call, gfsMap);
}

template<>
Expected<Resize> Factory<Resize>::operator()() const
{
	quint64 sizeMb = 0;
	po::variables_map::const_iterator argIter;
	if ((argIter = m_vm.find(OPT_SIZE)) == m_vm.end())
		return Expected<Resize>::fromMessage("Target size not found");
	Expected<quint64> size = parseSizeMb(QString::fromStdString(argIter->second.as<std::string>()));
	if (!size.isOk())
		return size;

	sizeMb = size.get();
	bool resizeLastPartition = m_vm.count(OPT_RESIZE_LAST_PARTITION);
	bool force = m_vm.count(OPT_FORCE);
	Expected<DiskAware> disk = Factory<DiskAware>::build(m_vm);
	if (!disk.isOk())
		return disk;

	return Resize(disk.get(), sizeMb, resizeLastPartition, force, m_gfsMap, m_call);
}

template<>
Expected<ResizeInfo> Factory<ResizeInfo>::operator()() const
{
	SizeUnitType unitType = SIZEUNIT_M;
	po::variables_map::const_iterator argIter;
	if ((argIter = m_vm.find(OPT_UNITS)) != m_vm.end() &&
		!determineSizeUnitType(QString::fromStdString(argIter->second.as<std::string>()), unitType))
		return Expected<ResizeInfo>::fromMessage("Cannot parse units");
	Expected<DiskAware> disk = Factory<DiskAware>::build(m_vm);
	if (!disk.isOk())
		return disk;

	return ResizeInfo(disk.get(), unitType);
}

template<>
Expected<Compact> Factory<Compact>::operator()() const
{
	bool force = m_vm.count(OPT_FORCE);
	Expected<DiskAware> disk = Factory<DiskAware>::build(m_vm);
	if (!disk.isOk())
		return disk;

	return Compact(disk.get(), force, m_call);
}

template<>
Expected<CompactInfo> Factory<CompactInfo>::operator()() const
{
	Expected<DiskAware> disk = Factory<DiskAware>::build(m_vm);
	if (!disk.isOk())
		return disk;

	return CompactInfo(disk.get());
}

template<>
Expected<MergeSnapshots> Factory<MergeSnapshots>::operator()() const
{
	Expected<DiskAware> disk = Factory<DiskAware>::build(m_vm);
	if (!disk.isOk())
		return disk;

	if (m_vm.count(OPT_EXTERNAL))
	{
		Expected<Merge::External::mode_type> mode =
			MergeSnapshots::getExternalMode(m_call);
		if (!mode.isOk())
			return mode;

		return MergeSnapshots(disk.get(), Merge::External::Executor(
					disk.get(), mode.get(), m_call), m_call);
	}
	else
	{
		return MergeSnapshots(disk.get(), Merge::Internal(
					disk.get(), m_call), m_call);
	}
}

template<>
Expected<Convert> Factory<Convert>::operator()() const
{
	Expected<DiskAware> disk = Factory<DiskAware>::build(m_vm);
	if (!disk.isOk())
		return disk;

	bool plain = m_vm.count(OPT_MAKE_PLAIN),
		 expanding = m_vm.count(OPT_MAKE_EXPANDING);
	if (!(plain ^ expanding))
	{
		return Expected<Convert>::fromMessage(
				"Either --plain or --expanding must be specified");
	}
	else if (plain)
		return Convert(disk.get(), Preallocation::Plain(disk.get(), m_call), m_call);
	else
		return Convert(disk.get(), Preallocation::Expanding(disk.get(), m_call), m_call);
}

} // namespace Command

////////////////////////////////////////////////////////////
// Visitor

Visitor::Visitor(const ParsedCommand &cmd,
				 const po::variables_map &vm,
				 const std::vector<std::string> &args):
	m_action(QString::fromStdString(cmd.getAction())), m_args(args)
{
	m_info = vm.count(OPT_INFO);
	// These options are not passed to commands.
	if (vm.count(OPT_NO_ACTION) == 0)
	{
		m_call = Call();
		m_gfsAction = Action();
	}
	m_gfsMap = GuestFS::Map(m_gfsAction);
	m_result = Expected<void>::fromMessage(QString("Unknown action: %1 %2").arg(
					m_action, m_info ? "--info" : ""));
}

Expected<Visitor> Visitor::create(const ParsedCommand &cmd)
{
	po::options_description options;
	options.add_options()
		("info,i", "Display estimates")
		("dry-run,n", "Do not actually do anything")
		("subargs", po::value<std::vector<std::string> >(), "Arguments for operation")
		;
	po::positional_options_description p;
	p.add("subargs", -1);
	po::variables_map vm;
	std::vector<std::string> args;

	try
	{
		po::parsed_options parsed = po::command_line_parser(cmd.getArgs())
			.options(options)
			.positional(p)
			.allow_unregistered()
			.run();
		po::store(parsed, vm);

		args = po::collect_unrecognized(
				parsed.options, po::include_positional);
	}
	catch (po::error &e)
	{
		return Expected<Visitor>::fromMessage(e.what());
	}
	return Visitor(cmd, vm, args);
}

template <typename T>
Expected<void> Visitor::createAndExecute() const
{
	Expected<Factory<T> > factory = Factory<T>::create(m_args, m_call, m_gfsMap);
	if (!factory.isOk())
		return factory;
	Expected<T> cmdRes = factory.get()();
	if (!cmdRes.isOk())
		return cmdRes;
	const T& cmd  = cmdRes.get();
	if (isPloop(cmd.getDiskPath()))
		return cmd.executePloop();
	return cmd.execute();
}

template Expected<void> Visitor::createAndExecute<Resize>() const;
template Expected<void> Visitor::createAndExecute<ResizeInfo>() const;
template Expected<void> Visitor::createAndExecute<Compact>() const;
template Expected<void> Visitor::createAndExecute<CompactInfo>() const;
template Expected<void> Visitor::createAndExecute<MergeSnapshots>() const;
template Expected<void> Visitor::createAndExecute<Convert>() const;

////////////////////////////////////////////////////////////
// UsageVisitor

UsageVisitor::UsageVisitor()
{
	m_result.add_options()
		("dry-run,n", "Do not actually do anything")
		;
}
