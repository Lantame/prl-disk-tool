///////////////////////////////////////////////////////////////////////////////
///
/// @file DiskLock.h
///
/// Hard disk lock/guard implementation
///
/// @modifier mperevedentsev
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
#ifndef DISK_LOCK_H
#define DISK_LOCK_H

#include <QString>
#include <QScopedPointer>
#include <QFile>

#include "Expected.h"

#include <boost/shared_ptr.hpp>

////////////////////////////////////////////////////////////
// DiskLock

struct DiskLock
{
	bool lock(const QString& path, int mode);
	void unlock();

private:
	QScopedPointer<QFile> m_file;
};

////////////////////////////////////////////////////////////
// DiskLockGuard

struct DiskLockGuard
{
	static Expected<boost::shared_ptr<DiskLockGuard> > openRead(const QString &path);
	static Expected<boost::shared_ptr<DiskLockGuard> > openWrite(const QString &path);

	~DiskLockGuard()
	{
		m_lock.unlock();
	}

private:
	DiskLockGuard()
	{
	}

	static Expected<boost::shared_ptr<DiskLockGuard> > create(const QString &path, int mode);

private:
	DiskLock m_lock;
};

#endif //DISK_LOCK_H
