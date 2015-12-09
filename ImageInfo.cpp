///////////////////////////////////////////////////////////////////////////////
///
/// @file ImageInfo.cpp
///
/// Parsing of qemu-img image and backing chain info.
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
#include <QStringList>
#include <QDir>

#include <boost/optional.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "ImageInfo.h"
#include "Util.h"
#include "StringTable.h"

namespace pt = boost::property_tree;
using namespace Image;

////////////////////////////////////////////////////////////
// Info

QString Info::toString() const
{
	QStringList lines;
	lines << QString("filename: ") + m_filename;
	lines << QString("virtualSize: ") + QString::number(m_virtualSize);
	lines << QString("actualSize: ") + QString::number(m_actualSize);
	lines << QString("format: ") + m_format;
	return lines.join("\n");
}

////////////////////////////////////////////////////////////
// Chain

QString Chain::toString() const
{
	QStringList images;
	Q_FOREACH(const Info &info, m_list)
		images << info.toString();
	return images.join("\n\n");
}

quint64 Chain::getActualSizeSum() const
{
	quint64 sum = 0;
	Q_FOREACH(const Info &info, m_list)
		sum += info.getActualSize();
	return sum;
}

quint64 Chain::getVirtualSizeMax() const
{
	quint64 max = 0;
	Q_FOREACH(const Info &info, m_list)
		max = qMax(max, info.getVirtualSize());
	return max;
}

////////////////////////////////////////////////////////////
// Parser

Expected<Info> Parser::parseInfo(const pt::ptree &v)
{
	boost::optional<std::string> filename = v.get_optional<std::string>("filename");
	if (!filename)
		return Expected<Info>::fromMessage(IDS_CANNOT_PARSE_IMAGE);
	boost::optional<quint64> virtualSize = v.get_optional<quint64>("virtual-size");
	if (!virtualSize)
		return Expected<Info>::fromMessage(IDS_CANNOT_PARSE_IMAGE);
	boost::optional<quint64> actualSize = v.get_optional<quint64>("actual-size");
	if (!actualSize)
		return Expected<Info>::fromMessage(IDS_CANNOT_PARSE_IMAGE);
	boost::optional<std::string> format = v.get_optional<std::string>("format");
	if (!format)
		return Expected<Info>::fromMessage(IDS_CANNOT_PARSE_IMAGE);
	if (*format != DISK_FORMAT)
	{
		return Expected<Info>::fromMessage(QString("%1: unsupported format \"%2\". Only \"%3\" is supported.")
										   .arg(QString::fromStdString(*filename))
										   .arg(QString::fromStdString(*format)).arg(DISK_FORMAT));
	}

	boost::optional<std::string> backing = v.get_optional<std::string>("backing-filename");
	if (backing)
	{
		boost::optional<std::string> fBacking = v.get_optional<std::string>("full-backing-filename");
		QString fullBacking;
		if (fBacking)
			fullBacking = QString::fromStdString(*fBacking);
		else if (QDir(QString::fromStdString(*backing)).isAbsolute())
			fullBacking = QString::fromStdString(*backing);
		else
		{
			// Images are in working directory.
			fullBacking = QDir::cleanPath(m_dirPath + "/" + QString::fromStdString(*backing));
		}
		return Info(QString::fromStdString(*filename), *virtualSize,
					*actualSize, QString::fromStdString(*format),
					QString::fromStdString(*backing), fullBacking);
	}
	return Info(QString::fromStdString(*filename), *virtualSize,
				*actualSize, QString::fromStdString(*format));
}

Expected<Chain> Parser::parse(const QByteArray &data)
{
	pt::ptree pt;
	std::istringstream stream(data.constData());
	try
	{
		read_json(stream, pt);
	}
	catch(pt::json_parser_error &e)
	{
		return Expected<Chain>::fromMessage(QString::fromStdString(e.what()));
	}

	QList<Info> chain;
	// Newest image is first, so add in reverse.
	Q_FOREACH(const pt::ptree::value_type &v, pt.get_child(""))
	{
		Expected<Info> result = parseInfo(v.second);
		if (!result.isOk())
			return result;
		chain.prepend(result.get());
	}
	return Chain(chain);
}
