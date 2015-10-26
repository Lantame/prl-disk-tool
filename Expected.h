///////////////////////////////////////////////////////////////////////////////
///
/// @file Expected.h
///
/// Wrapper containing object or error message/code.
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
#ifndef EXPECTED_H
#define EXPECTED_H

#include <QtGlobal>
#include <QString>

#include <boost/optional.hpp>
#include <boost/type_traits.hpp>
#include <boost/utility/enable_if.hpp>

template <typename T>
struct Expected
{
	Expected(const T &rhs):
		m_value(rhs), m_code(0)
	{
	}

	template <typename X>
	Expected(const Expected<X> &rhs, typename boost::enable_if<boost::is_convertible<X, T> >::type* dummy = 0)
	{
		Q_UNUSED(dummy);
		if (!rhs.isOk())
		{
			m_msg = rhs.getMessage();
			m_code = rhs.getCode();
			return;
		}
		m_value = rhs.get();
		m_code = 0;
	}

	template <typename X>
	Expected(const Expected<X> &rhs, typename boost::disable_if<boost::is_convertible<X, T> >::type* dummy = 0)
	{
		Q_UNUSED(dummy);
		if (!rhs.isOk())
		{
			m_msg = rhs.getMessage();
			m_code = rhs.getCode();
			return;
		}
		m_msg = "Invalid type conversion in Expected";
		m_code = -1;
	}

	static Expected fromMessage(const QString &msg, int code = -1)
	{
		Expected e;
		e.m_msg = msg;
		e.m_code = code;
		return e;
	}

	bool isOk() const
	{
		return m_value.is_initialized();
	}

	int getCode() const
	{
		return m_code;
	}

	const QString& getMessage() const
	{
		return m_msg;
	}

	const T& get() const
	{
		return m_value.get();
	}

	T& get()
	{
		return m_value.get();
	}

private:
	Expected():
		m_code(-1)
	{
	}

private:
	boost::optional<T> m_value;
	QString m_msg;
	int m_code;
};


template<>
struct Expected<void>
{
	Expected():
		m_code(0)
	{
	}

	Expected(const Expected &rhs):
		m_msg(rhs.m_msg), m_code(rhs.m_code)
	{
	}

	template <typename X>
	Expected(const Expected<X> &rhs)
	{
		if (!rhs.isOk())
		{
			m_msg = rhs.getMessage();
			m_code = rhs.getCode();
			return;
		}
		m_code = 0;
	}

	static Expected fromMessage(QString msg, int code = -1)
	{
		Expected e;
		e.m_msg = msg;
		e.m_code = code;
		return e;
	}

	bool isOk() const
	{
		return m_code == 0;
	}

	int getCode() const
	{
		return m_code;
	}

	const QString& getMessage() const
	{
		return m_msg;
	}

private:
	QString m_msg;
	int m_code;
};

#endif // EXPECTED_H
