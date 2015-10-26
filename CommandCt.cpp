///////////////////////////////////////////////////////////////////////////////
///
/// @file CommandCt.cpp
///
/// Command execution for container disk images (ploop).
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
#include <QFileInfo>

#include "Command.h"
#include "StringTable.h"
#include "Util.h"

using namespace Command;

namespace
{

char PLOOP[] = "/usr/sbin/ploop";
char DESCRIPTOR[] = "/DiskDescriptor.xml";

QString getDescriptor(const QString &path)
{
	return path + DESCRIPTOR;
}

} // namespace

Expected<void> Default::executePloop() const
{
	return Expected<void>::fromMessage("This action is not implemented for ploop");
}

Expected<void> Resize::executePloop() const
{
	QByteArray size = QString("%1M").arg(m_sizeMb).toUtf8();
	QByteArray path = getDescriptor(getDiskPath()).toUtf8();
	char p1[] = "resize", p2[] = "-s";
	char *args[] = {PLOOP, p1, p2, size.data(), path.data(), NULL};
	CallAdapter(m_call).execvp(PLOOP, args);
	return Expected<void>::fromMessage(IDS_ERR_PLOOP_EXEC_FAILED);
}

Expected<void> Compact::executePloop() const
{
	QByteArray path = getDescriptor(getDiskPath()).toUtf8();
	char p1[] = "balloon", p2[] = "discard", p3[] = "--automount", p4[] = "--defrag";
	char *args[] = {PLOOP, p1, p2, p3, p4, path.data(), NULL};
	CallAdapter(m_call).execvp(PLOOP, args);
	return Expected<void>::fromMessage(IDS_ERR_PLOOP_EXEC_FAILED);
}

Expected<void> MergeSnapshots::executePloop() const
{
	QByteArray path = getDescriptor(getDiskPath()).toUtf8();
	char p1[] = "snapshot-merge", p2[] = "-A";
	char *args[] = {PLOOP, p1, p2, path.data(), NULL};
	CallAdapter(m_call).execvp(PLOOP, args);
	return Expected<void>::fromMessage(IDS_ERR_PLOOP_EXEC_FAILED);
}
