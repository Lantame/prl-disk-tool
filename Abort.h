///////////////////////////////////////////////////////////////////////////////
///
/// @file Abort.h
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
#ifndef ABORT_H
#define ABORT_H

#include <boost/shared_ptr.hpp>

#include <QSharedPointer>
#include <QMutex>
#include <QFuture>

namespace Abort
{

////////////////////////////////////////////////////////////
// Token

struct Token
{
	typedef boost::shared_ptr<Token> token_type;

	void requestCancellation()
	{
		m_value = true;
	}
	bool isCancellationRequested() const
	{
		return m_value;
	}

private:
	bool volatile m_value;
};

typedef Token::token_type token_type;

////////////////////////////////////////////////////////////
// Signal

struct Signal
{
	Signal();
	~Signal();

	void start();
	void stop();
	void set(const token_type& token)
	{
		m_token = token;
	}

private:
	void wait();

	QMutex m_mutex;
	QFuture<void> m_result;
	sigset_t m_backup;
	token_type m_token;
};

} // namespace Abort

#endif // ABORT_H
