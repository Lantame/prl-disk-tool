///////////////////////////////////////////////////////////////////////////////
///
/// @file StringTable.cpp
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

#include "StringTable.h"

char IDS_ERR_INVALID_OPTION[] = "Invalid option '%1'";
char IDS_CANNOT_PARSE_IMAGE[] = "Cannot parse image information";
char IDS_ERR_NO_FREE_SPACE[] = "Not enough free space ( needed: %1 available: %2 )";
char IDS_ERR_SUBPROGRAM_RETURN_CODE[] = "%1 %2 returned %3";
char IDS_ERR_INVALID_ARGS[] = "Invalid arguments";
char IDS_ERR_INVALID_HDD[] = "Invalid disk path";
char IDS_ERR_HDD_NOT_EXISTS[] = "The specified disk image \"%1\" does not exist";
char IDS_ERR_HAS_INTERNAL_SNAPSHOTS[] = "Image has internal snapshots. Merge snapshots to proceed.";
char IDS_ERR_PLOOP_EXEC_FAILED[] = "Failed to execute ploop";
char IDS_ERR_NO_FS_FREE_SPACE[] =
	"Not enough free space on filesystem ( requested: %1 minimum: %2 )\n"
	"Free at least %3 bytes more";
char IDS_ERR_CANNOT_GET_PART_FS[] = "Unable to get filesystem for partition";
char IDS_ERR_CANNOT_GET_PART_LIST[] = "Unable to get partition list";
char IDS_ERR_CANNOT_MOUNT[] = "Cannot mount the partition";
char IDS_ERR_CANNOT_PARSE_MIN_SIZE[] = "Cannot get minimum size from output";
char IDS_ERR_CANNOT_GET_MIN_SIZE[] = "Cannot get minimum size of filesystem";
char IDS_ERR_FS_UNSUPPORTED[] = "Unsupported filesystem: %1";


char IDS_DISK_INFO__BLOCK_SIZE[] = "        Block size:       ";
char IDS_DISK_INFO__BLOCKS_TOTAL[] = "        Total blocks:     ";
char IDS_DISK_INFO__BLOCKS_ALLOCATED[] = "        Allocated blocks: ";
char IDS_DISK_INFO__BLOCKS_USED[] = "        Used blocks:      ";

char IDS_DISK_INFO__HEAD[] = "Disk information:";
char IDS_DISK_INFO__SIZE[] = "\tSize:\t\t\t\t\t\t";
char IDS_DISK_INFO__MIN[] = "\tMinimum:\t\t\t\t\t";
char IDS_DISK_INFO__MAX[] = "\tMaximum:\t\t\t\t\t";
char IDS_DISK_INFO__MIN_KEEP_FS[] = "\tMinimum without resizing the last partition:\t";

char IDS_DISK_INFO__RESIZE_WARN_FS_NOTSUPP[] =
	"    Warning! The last partition cannot be resized "
	"because its file system is either not supported or damaged.";

