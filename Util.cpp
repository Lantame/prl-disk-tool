///////////////////////////////////////////////////////////////////////////////
///
/// @file Util.cpp
///
/// Subprogram execution
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
#include <cstdio>

#include <QProcess>

#include "Util.h"

extern const char QEMU_IMG[] = "/usr/bin/qemu-img";
extern const char DISK_FORMAT[] = "qcow2";
extern const char DESCRIPTOR[] = "DiskDescriptor.xml";

bool Logger::s_verbose = false;

namespace
{
enum {CMD_WORK_TIMEOUT = 60 * 60 * 1000};
}


int run_prg(const char *name, const QStringList &lstArgs, QByteArray *out, QByteArray *err)
{
	QProcess proc;
	proc.start(name, lstArgs);
	if (!proc.waitForFinished(CMD_WORK_TIMEOUT))
	{
		fprintf(stderr, "%s tool not responding. Terminate it now.", name);
		proc.kill();
		return -1;
	}
	if (out)
		*out = proc.readAllStandardOutput();
	if (err)
		*err = proc.readAllStandardError();

	if (0 != proc.exitCode())
	{
		fprintf(stderr, "%s utility failed: %s %s [%d]\nout=%s\nerr=%s",
				name, name,
				QSTR2UTF8(lstArgs.join(" ")),
				proc.exitCode(),
				out ? out->data() : proc.readAllStandardOutput().data(),
				err ? err->data() : proc.readAllStandardError().data());
		return -1;
	}
	return 0;
}
