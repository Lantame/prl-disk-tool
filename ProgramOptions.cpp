///////////////////////////////////////////////////////////////////////////////
///
/// @file ProgramOptions.cpp
///
/// Option parser implementation.
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
#include <string>

#include "Expected.h"
#include "ProgramOptions.h"
#include "Util.h"

namespace po = boost::program_options;

extern const char OPT_SHMEM[] = "comm";
extern const char OPT_TR_ERRORS[] = "";
extern const char OPT_NO_ACTION[] = "dry-run";
extern const char OPT_VERBOSE[] = "verbose";

// operation specification
extern const char OPT_OPERATION[] = "operation";
extern const char OPT_HELP[] = "help";
extern const char OPT_USAGE[] = "usage"; // alias

// additional (optional and mandatory) parameters
extern const char OPT_DISKPATH[] = "hdd";
extern const char OPT_SIZE[] = "size";
extern const char OPT_RESIZE_LAST_PARTITION[] = "resize_partition";
extern const char OPT_RESIZE_ROUND_DOWN[] = "";
extern const char OPT_FORCE[] = "force"; // force suspended disks to resize
extern const char OPT_INFO[] = "info";
extern const char OPT_UNITS[] = "units";
extern const char OPT_HUMAN_READABLE[] = "";
extern const char OPT_EXTERNAL[] = "external";


OptionParser::OptionParser()
{
	po::options_description generic("Generic options");
	generic.add_options()
		("help,h", "Produce help message")
		("usage", "Produce help message")
		("verbose,v", "Enable information messages")
		("comm", po::value<std::string>(), "Shared memory name")
		("operation", po::value<std::string>(), "Operation to perform")
		("subargs", po::value<std::vector<std::string> >(), "Arguments for operation")
		;
	m_generic.add(generic);
}

Expected<void> OptionParser::parseOptions(int argc, const char * const *argv)
{
	po::positional_options_description p;
	p.add(OPT_OPERATION, 1).add("subargs", -1);

	try
	{
		po::parsed_options parsed = po::command_line_parser(argc, argv)
			.options(m_generic)
			.positional(p)
			.allow_unregistered()
			.run();

		po::store(parsed, m_vm);
		if (!m_vm.count(OPT_OPERATION) && !(m_vm.count(OPT_HELP)) && !(m_vm.count(OPT_USAGE)))
			return Expected<void>::fromMessage("No operation specified");

		m_unrecognized = po::collect_unrecognized(
					parsed.options, po::include_positional);
	}
	catch (po::error &e)
	{
		return Expected<void>::fromMessage(e.what());
	}

	return Expected<void>();
}

ParsedCommand OptionParser::getCommand() const
{
	if (!m_vm.count(OPT_OPERATION))
		return ParsedCommand("", m_unrecognized, m_vm);
	return ParsedCommand(m_vm[OPT_OPERATION].as<std::string>(), m_unrecognized, m_vm);
}

Expected<ParsedCommand> OptionParser::parseCommand(int argc, const char * const *argv)
{
	Expected<void> result = parseOptions(argc, argv);
	if (!result.isOk())
		return result;

	return getCommand();
}

void OptionParser::printUsage(const po::options_description &options) const
{
	po::options_description usage;
	usage.add(po::options_description(
				"Usage:\n\tprl_disk_tool <operation> [<arguments>]"));

	po::options_description generic("Generic options");
	generic.add_options()
		("help,h", "Produce help message")
		("usage", "Produce help message")
		("verbose,v", "Enable information messages")
		("comm", po::value<std::string>(), "Shared memory name (currently unused)")
		;
	usage.add(generic);

	usage.add(options);

	std::stringstream ss;
	ss << usage << std::endl;
	Logger::print(QString::fromStdString(ss.str()));
}
