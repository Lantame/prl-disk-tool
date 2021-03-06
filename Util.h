///////////////////////////////////////////////////////////////////////////////
///
/// @file Util.h
///
/// Utility macros, logging and subprogram execution.
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

#ifndef UTIL_H
#define UTIL_H

#include "Abort.h"

#include <QStringList>
#include <QByteArray>
#include <QFile>
#include <iostream>

#include <unistd.h>

#include <boost/optional.hpp>

#define QSTR2UTF8(str) ( (str).toUtf8().constData() )
#define UTF8_2QSTR(str) QString::fromUtf8( (str) )

/** Unit types for command line interface */
enum SizeUnitType
{
	SIZEUNIT_b,  // bytes

	SIZEUNIT_K,  // kilobytes
	SIZEUNIT_M,  // megabytes
	SIZEUNIT_G,  // gigabytes
	SIZEUNIT_T,  // terabytes

	SIZEUNIT_s   // sectors
};

extern const char QEMU_IMG[];
extern const char DISK_FORMAT[];
extern const char DESCRIPTOR[];

enum {CMD_WORK_TIMEOUT = 60 * 60};

int run_prg(const char *name, const QStringList &lstArgs,
            QByteArray *out = NULL, QByteArray *err = NULL,
            unsigned timeout = CMD_WORK_TIMEOUT,
            const Abort::token_type &token = Abort::token_type());

////////////////////////////////////////////////////////////
// Logger

struct Logger: std::ostream
{
	static void init(bool verbose)
	{
		s_verbose = verbose;
	}

	static void print(const QString &line, std::ostream &stream = std::cout)
	{
		stream << QSTR2UTF8(line) << std::endl;
	}

	static void info(const QString &line)
	{
		if (s_verbose)
			print(line);
	}

	static void error(const QString &line)
	{
		print(line, std::cerr);
	}

private:
	static bool s_verbose;
};

////////////////////////////////////////////////////////////
// Call

struct Call
{
	Call()
	{
	}

	Call(const Abort::token_type &token): m_token(token)
	{
	}

	bool rename(const QString &oldName, const QString &newName) const
	{
		if (QFile::exists(newName))
			QFile::remove(newName);
		return QFile::rename(oldName, newName);
	}

	bool remove(const QString &name) const
	{
		return QFile::remove(name);
	}

	int run(const char *name, const QStringList &lstArgs,
	        QByteArray *out, QByteArray *err, unsigned timeout) const
	{
		return run_prg(name, lstArgs, out, err, timeout, m_token);
	}

private:
	Abort::token_type m_token;
};

////////////////////////////////////////////////////////////
// CallAdapter

struct CallAdapter
{
	explicit CallAdapter(const boost::optional<Call> &call):
		m_call(call)
	{
	}

	bool rename(const QString &oldName, const QString &newName) const
	{
		Logger::info(QString("mv %1 %2").arg(oldName).arg(newName));
		return !(bool)m_call || m_call->rename(oldName, newName);
	}

	bool remove(const QString &name) const
	{
		Logger::info(QString("rm %1").arg(name));
		return !(bool)m_call || m_call->remove(name);
	}

	int run(const char *name, const QStringList &lstArgs,
	        QByteArray *out = NULL, QByteArray *err = NULL,
	        unsigned timeout = CMD_WORK_TIMEOUT) const
	{
		Logger::info(QString("%1 %2 (timeout %3)")
		             .arg(name).arg(lstArgs.join(" ")).arg(timeout));
		return (bool)m_call ? m_call->run(name, lstArgs, out, err, timeout) : 0;
	}

	int execvp(const char *name, char * const *args) const
	{
		QString log;
		for (char * const *cur = args; *cur != NULL; ++cur)
			log += QString("%1 ").arg(*cur);
		Logger::info(log);
		if (!(bool)m_call)
			exit(0);
		return ::execvp(name, args);
	}

	bool hasCall() const
	{
		return m_call;
	}

private:
	boost::optional<Call> m_call;
};

#endif // UTIL_H
