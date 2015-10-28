///////////////////////////////////////////////////////////////////////////////
///
/// @file main.cpp
///
/// prl_disk_tool entry point and initialization
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

#include <iostream>

#include <QString>

#include <boost/mpl/vector.hpp>
#include <boost/mpl/for_each.hpp>

#include "Command.h"
#include "Util.h"
#include "ProgramOptions.h"

namespace
{

typedef boost::mpl::vector<
	Command::Traits<Command::Resize>,
	Command::Traits<Command::ResizeInfo>,
	Command::Traits<Command::Compact>,
	Command::Traits<Command::CompactInfo>,
	Command::Traits<Command::MergeSnapshots>,
	Command::Traits<Command::Convert>
		> desc_type;

void printUsage(const OptionParser &parser)
{
	UsageVisitor v;
	boost::mpl::for_each<desc_type>(boost::ref(v));
	parser.printUsage(v.getResult());
}

} //namespace

int main(int argc, char *argv[])
{
	OptionParser parser;

	Expected<ParsedCommand> parsed = parser.parseCommand(argc, argv);
	if (!parsed.isOk())
	{
		Logger::error(parsed.getMessage());
		printUsage(parser);
		return parsed.getCode();
	}

	ParsedCommand command = parsed.get();
	Logger::init(command.isVerbose());
	if (command.isUsageIssued())
	{
		printUsage(parser);
		return 0;
	}

	Expected<Visitor> vRes = Visitor::create(command);
	if (!vRes.isOk())
	{
		Logger::error(vRes.getMessage());
		printUsage(parser);
		return vRes.getCode();
	}
	Visitor &v = vRes.get();
	boost::mpl::for_each<desc_type>(boost::ref(v));

	Expected<void> result = v.getResult();
	if (!result.isOk())
	{
		Logger::error(result.getMessage());
		return result.getCode();
	}

	return 0;
}
