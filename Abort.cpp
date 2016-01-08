///////////////////////////////////////////////////////////////////////////////
///
/// @file Abort.cpp
///
/// @author dandreev
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

#include <unistd.h>
#include <signal.h>

#include <QtConcurrentRun>
#include <QMutexLocker>

#include "Abort.h"
#include "Util.h"

namespace Abort
{

////////////////////////////////////////////////////////////
// Signal

Signal::Signal()
{
	sigset_t a;

	sigfillset(&a);
	sigdelset(&a, SIGCHLD);

	sigprocmask(SIG_BLOCK, &a, &m_backup);
}

void Signal::start()
{
	QMutexLocker m(&m_mutex);

	if (!m_result.isFinished())
		return;

	timespec t = {0, 0};
	sigset_t a;

	sigemptyset(&a);
	sigaddset(&a, SIGUSR1);

	while (SIGUSR1 == sigtimedwait(&a, NULL, &t))
	{
	}

	m_result = QtConcurrent::run(this, &Signal::wait);
}

void Signal::stop()
{
	QMutexLocker m(&m_mutex);
	if (m_result.isFinished())
		return;

	kill(getpid(), SIGUSR1);
	m_result.waitForFinished();
}

void Signal::wait()
{
	sigset_t a, b;
	timespec t = {0, 0};

	sigfillset(&a);
	sigprocmask(SIG_BLOCK, &a, NULL);

	sigemptyset(&a);
	sigaddset(&a, SIGTERM);
	sigaddset(&a, SIGINT);
	sigaddset(&a, SIGUSR1);

	sigemptyset(&b);
	sigaddset(&b, SIGTERM);
	sigaddset(&b, SIGINT);

	switch (sigwaitinfo(&a, NULL))
	{
	case SIGUSR1:
		if (-1 == sigtimedwait(&b, NULL, &t))
			break;
	case SIGINT:
	case SIGTERM:
		Logger::info("Terminate");
		if (m_token)
			m_token->requestCancellation();
		break;
	}
}

Signal::~Signal()
{
	stop();
	sigprocmask(SIG_BLOCK, &m_backup, NULL);
}

} // namespace Abort

