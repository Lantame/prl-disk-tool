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
#include <signal.h>

#include <QProcess>

#include "Util.h"

extern const char QEMU_IMG[] = "/usr/bin/qemu-img";
extern const char DISK_FORMAT[] = "qcow2";
extern const char DESCRIPTOR[] = "DiskDescriptor.xml";

bool Logger::s_verbose = false;

namespace
{
enum {CMD_WORK_STEPS = 60 * 60, CMD_WORK_STEP_TIME = 1000};

class IndependentProcess: public QProcess
{
protected:
	void setupChildProcess()
	{
		setpgid(0, 0);

		sigset_t a;
		sigemptyset(&a);
		sigprocmask(SIG_SETMASK, &a, NULL);
	}
};

}


int run_prg(const char *name, const QStringList &lstArgs, QByteArray *out, QByteArray *err, const Abort::token_type &token)
{
	IndependentProcess proc;
	proc.start(name, lstArgs);

	int step;
	for (step = 0; step < CMD_WORK_STEPS; ++step)
	{
		if (proc.waitForFinished(CMD_WORK_STEP_TIME))
			break;

		if (token && token->isCancellationRequested())
		{
			fprintf(stderr, "Execution of '%s' has been cancelled. Terminate it now.\n", name);
			proc.kill();
			proc.waitForFinished();
			return -1;
		}
	}

	if (step >= CMD_WORK_STEPS)
	{
		fprintf(stderr, "%s tool not responding. Terminate it now.", name);
		proc.kill();
		proc.waitForFinished();
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
