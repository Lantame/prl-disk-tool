///////////////////////////////////////////////////////////////////////////////
///
/// @file Lvm.cpp
///
/// Structured representation of LVM configuration and parsing.
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
#include <QTemporaryFile>
#include <QSet>
#include <QFileInfo>
#include <boost/optional.hpp>

#include "Lvm.h"
#include "Util.h"

using namespace Lvm;

namespace
{
	const char PARSER[] = "/usr/share/prl-disk-tool/lvm_parser.py";
} // namespace

////////////////////////////////////////////////////////////
// Physical

boost::optional<Segment> Physical::getLastSegment() const
{
	if (m_segments.isEmpty())
		return boost::none;
	return *std::max_element(m_segments.begin(),
	                         m_segments.end(), Segment::Before());
}

////////////////////////////////////////////////////////////
// Config

Expected<Config> Config::create(const QString &config, const QString &group)
{
	QTemporaryFile file("/tmp/lvm.conf.XXXXXX");
	bool result = file.open();
	if (!result)
		return Expected<Config>::fromMessage("Unable to store config file");
	qint64 ret = file.write(config.toUtf8());
	if (ret < 0)
		return Expected<Config>::fromMessage("Unable to store config file");
	file.close();
	QStringList args = QStringList() << file.fileName() << group;
	QByteArray out;
	ret = run_prg(PARSER, args, &out);
	if (ret)
		return Expected<Config>::fromMessage("Unable to parse config file");
	return parseOutput(out);
}

Physical Config::getPhysical(const QString &partition) const
{
	QList<Segment> matched;
	Q_FOREACH(const Segment &segment, m_segments)
	{
		if (segment.getPhysical() == partition)
			matched << segment;
	}
	return Physical(m_group, matched);
}

QStringList Config::getPhysicals() const
{
	QSet<QString> result;
	Q_FOREACH(const Segment &segment, m_segments)
		result.insert(segment.getPhysical());
	return result.toList();
}

Expected<Config> Config::parseOutput(const QByteArray &out)
{
	// VG and LV identifiers may contain only symbols from [a-zA-Z0-9._+-].
	// Thus using spaces as separators is safe.
	//               |    vg_name       |ExtSizeInSec|  attrs |
	QRegExp groupRE("^([a-zA-Z0-9._+-]+)\\s+(\\d+)\\s+(.*)\\s*$");
	//                 |  lv_name         |segmentId|    linear   |lastInLogical|pv_name(partition)|startOffset|endOffset|    attrs    |
	QRegExp segmentRE("^([a-zA-Z0-9._+-]+):(\\d+)\\s+(linear|stripped)\\s+(last)?\\s*(/dev/sd[a-z]\\d+)\\[(\\d+)\\.\\.(\\d+)\\]\\s+(.*)\\s*$");

	QStringList lines = QString(out).split('\n', QString::SkipEmptyParts);
	QList<Segment> segments;
	boost::optional<Group> group;
	Q_FOREACH(const QString &line, lines)
	{
		if (segmentRE.indexIn(line) != -1)
		{
			Logger::info(QString("Lvm parser: %1").arg(line));
			Logical logical(segmentRE.cap(1), segmentRE.cap(8));
			Segment segment(logical,
							segmentRE.cap(2).toUInt(),
							segmentRE.cap(3) == "linear",
							segmentRE.cap(4) == "last",
							segmentRE.cap(5),
							segmentRE.cap(6).toULongLong(),
							segmentRE.cap(7).toULongLong());
			segments << segment;
		}
		else if (groupRE.indexIn(line) != -1)
		{
			Logger::info(QString("Lvm parser: %1").arg(line));
			group = Group(groupRE.cap(1), groupRE.cap(2).toULongLong(), groupRE.cap(3));
		}
		else
		{
			Logger::error(QString("Unable to parse line from %1:\n'%2'").arg(PARSER, line));
		}
	}

	if (!group)
		return Expected<Config>::fromMessage("No LVM group found");

	return Config(*group, segments);
}
