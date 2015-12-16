///////////////////////////////////////////////////////////////////////////////
///
/// @file StringTable.h
///
/// This file contains all text strings which
///     are used in prl_disk_tool utility
///     are visible for customer
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

#ifndef RESIZER_STRING_TABLE_H
#define RESIZER_STRING_TABLE_H

extern char IDS_ERR_INVALID_OPTION[];
extern char IDS_CANNOT_PARSE_IMAGE[];
extern char IDS_ERR_NO_FREE_SPACE[];
extern char IDS_ERR_SUBPROGRAM_RETURN_CODE[];
extern char IDS_ERR_INVALID_ARGS[];
extern char IDS_ERR_INVALID_HDD[];
extern char IDS_ERR_HDD_NOT_EXISTS[];
extern char IDS_ERR_CANNOT_CONVERT_NEED_MERGE[];
extern char IDS_ERR_HAS_INTERNAL_SNAPSHOTS[];
extern char IDS_ERR_PLOOP_EXEC_FAILED[];
extern char IDS_ERR_NO_FS_FREE_SPACE[];
extern char IDS_ERR_CANNOT_GET_PART_FS[];
extern char IDS_ERR_CANNOT_GET_PART_LIST[];
extern char IDS_ERR_CANNOT_MOUNT[];
extern char IDS_ERR_CANNOT_PARSE_MIN_SIZE[];
extern char IDS_ERR_CANNOT_GET_MIN_SIZE[];
extern char IDS_ERR_FS_UNSUPPORTED[];

extern char IDS_DISK_INFO__BLOCK_SIZE[];
extern char IDS_DISK_INFO__BLOCKS_TOTAL[];
extern char IDS_DISK_INFO__BLOCKS_ALLOCATED[];
extern char IDS_DISK_INFO__BLOCKS_USED[];

extern char IDS_DISK_INFO__HEAD[];
extern char IDS_DISK_INFO__SIZE[];
extern char IDS_DISK_INFO__MIN[];
extern char IDS_DISK_INFO__MAX[];
extern char IDS_DISK_INFO__MIN_KEEP_FS[];

extern char IDS_DISK_INFO__RESIZE_WARN_FS_NOTSUPP[];

#endif // RESIZER_STRING_TABLE_H

