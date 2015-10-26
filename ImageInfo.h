///////////////////////////////////////////////////////////////////////////////
///
/// @file ImageInfo.h
///
/// qemu-img image and backing chain information.
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
#ifndef IMAGE_INFO_H
#define IMAGE_INFO_H

#include <QString>
#include <QByteArray>

#include <boost/property_tree/ptree.hpp>

#include "Expected.h"

namespace Image
{

////////////////////////////////////////////////////////////
// Info

struct Info
{
	Info(const QString& filename, quint64 virtualSize,
		 quint64 actualSize, const QString &format):
		m_filename(filename), m_virtualSize(virtualSize),
		m_actualSize(actualSize), m_format(format)
	{
	}

	// backingFilename only with fullBackingFilename
	Info(const QString& filename, quint64 virtualSize,
		 quint64 actualSize, const QString &format,
		 const QString &backingFilename,
		 const QString &fullBackingFilename):
		m_filename(filename), m_virtualSize(virtualSize),
		m_actualSize(actualSize), m_format(format),
		m_backingFilename(backingFilename),
		m_fullBackingFilename(fullBackingFilename)
	{
	}

	const QString& getFilename() const
	{
		return m_filename;
	}

	quint64 getVirtualSize() const
	{
		return m_virtualSize;
	}

	quint64 getActualSize() const
	{
		return m_actualSize;
	}

	const QString& getFormat() const
	{
		return m_format;
	}

	const QString& getBackingFilename() const
	{
		return m_backingFilename;
	}

	const QString& getFullBackingFilename() const
	{
		return m_fullBackingFilename;
	}

	QString toString() const;

private:
	QString m_filename;
	quint64 m_virtualSize;
	quint64 m_actualSize;
	QString m_format;
	QString m_backingFilename;
	QString m_fullBackingFilename;
};

////////////////////////////////////////////////////////////
// Chain

struct Chain
{
	typedef QList<Info>::const_iterator const_iterator;

	Chain(const QList<Info> &list):
		m_list(list)
	{
	}

	const QList<Info>& getList() const
	{
		return m_list;
	}

	QString toString() const;

	quint64 getActualSizeSum() const;
	quint64 getVirtualSizeMax() const;

private:
	QList<Info> m_list;
};

////////////////////////////////////////////////////////////
// Parser

struct Parser
{
	Parser(const QString &dirPath):
		m_dirPath(dirPath)
	{
	}

	/* Returns chain of backing images, from oldest to newest. */
	Expected<Chain> parse(const QByteArray &data);

private:
	Expected<Info> parseInfo(const boost::property_tree::ptree &pt);

	QString m_dirPath;
};

} // namespace Image

#endif // IMAGE_INFO_H
