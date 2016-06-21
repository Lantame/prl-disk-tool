///////////////////////////////////////////////////////////////////////////////
///
/// @file Lvm.h
///
/// Structured representation of LVM configuration.
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
#ifndef LVM_H
#define LVM_H

#include <QList>

#include "Expected.h"


namespace Lvm
{

////////////////////////////////////////////////////////////
// Group

struct Group
{
	Group(QString name, quint64 extentSize, const QString &attributes):
		m_name(name), m_extentSize(extentSize), m_attributes(attributes)
	{
	}

	bool isResizeable() const
	{
		return m_attributes.contains("RESIZEABLE");
	}

	bool isReadable() const
	{
		return m_attributes.contains("READ");
	}

	bool isWriteable() const
	{
		return m_attributes.contains("WRITE");
	}

	quint64 getExtentSizeInSectors() const
	{
		return m_extentSize;
	}

	const QString& getName() const
	{
		return m_name;
	}

private:
	QString m_name;
	quint64 m_extentSize;
	QString m_attributes;
};

////////////////////////////////////////////////////////////
// Logical

struct Logical
{
	Logical(const QString &name, const QString &attributes):
		m_name(name), m_attributes(attributes)
	{
	}

	QString getName() const
	{
		return m_name;
	}

	bool isWriteable() const
	{
		return m_attributes.contains("WRITE");
	}

private:
	QString m_name;
	QString m_attributes;
};

////////////////////////////////////////////////////////////
// Segment

struct Segment
{
	Segment(Logical logical, unsigned index,
			bool linear, bool lastInLogical,
			QString physical,
			quint64 startOffset, quint64 endOffset):
		m_logical(logical), m_index(index),
		m_linear(linear), m_lastInLogical(lastInLogical),
		m_physical(physical), m_startOffset(startOffset), m_endOffset(endOffset)
	{
	}

	quint64 getSizeInExtents() const
	{
		return m_endOffset - m_startOffset + 1;
	}

	quint64 getStartInExtents() const
	{
		return m_startOffset;
	}

	quint64 getEndInExtents() const
	{
		return m_endOffset;
	}

	bool isLinear() const
	{
		return m_linear;
	}

	bool isLastInLogical() const
	{
		return m_lastInLogical;
	}

	const Logical& getLogical() const
	{
		return m_logical;
	}

	bool isResizeable() const
	{
		// Stripped or not-last segments or segments in unmodifiable group.
		return isLinear() && isLastInLogical() && getLogical().isWriteable();
	}

	const QString& getPhysical() const
	{
		return m_physical;
	}

	struct Before
	{
		bool operator() (const Segment& first, const Segment &second)
		{
			return first.m_endOffset < second.m_endOffset;
		}
	};

private:
	Logical m_logical;
	unsigned m_index;
	bool m_linear;
	bool m_lastInLogical;
	QString m_physical;
	// Offsets of segment in physical (in extents).
	quint64 m_startOffset;
	quint64 m_endOffset;
};

////////////////////////////////////////////////////////////
// Physical

struct Physical
{
	Physical(const Group &group, const QList<Segment> &segments):
		m_group(group), m_segments(segments)
	{
	}

	boost::optional<Segment> getLastSegment() const;

	const Group& getGroup() const
	{
		return m_group;
	}

private:
	Group m_group;
	QList<Segment> m_segments;
};

////////////////////////////////////////////////////////////
// Config

struct Config
{
	// Save config as temporary file and run parser.
	static Expected<Config> create(const QString &config, const QString &group);

	const Group& getGroup() const
	{
		return m_group;
	}

	Physical getPhysical(const QString &partition) const;
	QStringList getPhysicals() const;

private:
	Config(const Group &group, const QList<Segment> &segments):
		m_group(group), m_segments(segments)
	{
	}

	static Expected<Config> parseOutput(const QByteArray &out);

	Group m_group;
	QList<Segment> m_segments;
};

} // namespace Lvm

#endif // LVM_H
