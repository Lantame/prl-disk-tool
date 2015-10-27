///////////////////////////////////////////////////////////////////////////////
///
/// @file ProgramOptions.h
///
/// Options declaration and parser.
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
#ifndef PROGRAM_OPTIONS_H
#define PROGRAM_OPTIONS_H

#include <map>

#include <QString>

#include <boost/program_options.hpp>

#include "Expected.h"

extern const char OPT_SHMEM[];
extern const char OPT_TR_ERRORS[];
extern const char OPT_NO_ACTION[];
extern const char OPT_VERBOSE[];

// operation specification
extern const char OPT_OPERATION[];
extern const char OPT_HELP[];
extern const char OPT_USAGE[]; // alias

// additional (optional and mandatory) parameters
extern const char OPT_DISKPATH[];
extern const char OPT_SIZE[];
extern const char OPT_RESIZE_LAST_PARTITION[];
extern const char OPT_RESIZE_ROUND_DOWN[];
extern const char OPT_FORCE[]; // force suspended disks to resize
extern const char OPT_INFO[];
extern const char OPT_UNITS[];
extern const char OPT_HUMAN_READABLE[];
extern const char OPT_EXTERNAL[];

// disk options
extern const char OPT_MAKE_EXPANDING[];
extern const char OPT_MAKE_PLAIN[];

////////////////////////////////////////////////////////////
// ParsedCommand

struct ParsedCommand
{
	ParsedCommand(const std::string &action, const std::vector<std::string> &args,
				  const boost::program_options::variables_map &parsed):
		m_action(action), m_args(args), m_parsed(parsed)
	{
	}

	const std::string& getAction() const
	{
		return m_action;
	}

	const std::vector<std::string>& getArgs() const
	{
		return m_args;
	}

	bool isVerbose() const
	{
		return m_parsed.count(OPT_VERBOSE);
	}

	bool isUsageIssued() const
	{
		return m_parsed.count(OPT_USAGE) || m_parsed.count(OPT_HELP);
	}

private:
	std::string m_action;
	std::vector<std::string> m_args;
	boost::program_options::variables_map m_parsed;
};

////////////////////////////////////////////////////////////
// OptionParser

struct OptionParser
{
	OptionParser();

	Expected<ParsedCommand> parseCommand(int argc, const char * const *argv);

	void printUsage(const boost::program_options::options_description &options) const;

private:
	Expected<void> parseOptions(int argc, const char * const *argv);

	ParsedCommand getCommand() const;

	std::map<std::string, boost::program_options::options_description> m_operationOptions;
	boost::program_options::options_description m_generic;
	boost::program_options::variables_map m_vm;
	std::vector<std::string> m_unrecognized;
};

#endif // PROGRAM_OPTIONS_H
