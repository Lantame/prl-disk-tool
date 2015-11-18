///////////////////////////////////////////////////////////////////////////////
///
/// @file DiskLock.cpp
///
/// Disk lock implementation
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

#include <sys/file.h>

#include "DiskLock.h"
#include "Util.h"

////////////////////////////////////////////////////////////
// DiskLock

/**
 * Lock disk from others and deny access to its files
 * Note, that path should be correct at call
 */
bool DiskLock::lock(const QString& path, int mode)
{
	Logger::info(QString("Disk lock: %1").arg(path));

	m_file.reset(new QFile(path));
	if (!m_file->open(QIODevice::ReadWrite))
		return false;

	// Open file exclusively
	if (flock(m_file->handle(), mode | LOCK_NB))
	{
		m_file->close();
		return false;
	}

	return true;
}

/**
 * Unlock disk and grant access to others
 * Safe to call if 'lock' failed.
 */
void DiskLock::unlock()
{
	Logger::info("Disk unlock");
	m_file->close();
}

////////////////////////////////////////////////////////////
// DiskLockGuard

Expected<boost::shared_ptr<DiskLockGuard> > DiskLockGuard::create(const QString &path, int mode)
{
	boost::shared_ptr<DiskLockGuard> guard(new DiskLockGuard());

	if (!guard->m_lock.lock(path, mode))
	{
		// set_shared_error(res);
		return Expected<boost::shared_ptr<DiskLockGuard> >::fromMessage(
				QString("The specified disk image \"%1\" is locked by another process").arg(path));
	}
	return guard;
}

Expected<boost::shared_ptr<DiskLockGuard> > DiskLockGuard::openRead(const QString &path)
{
	return create(path, LOCK_SH);
}

Expected<boost::shared_ptr<DiskLockGuard> > DiskLockGuard::openWrite(const QString &path)
{
	return create(path, LOCK_EX);
}
