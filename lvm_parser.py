#!/usr/bin/python

import sys
import re
import json
import pprint

def strip_comment(line):
	if line.find("#") == -1:
		return line
	# strip commentary
	parts = line.split("\"")
	index = 0
	for part_index, part in enumerate(parts):
		if not (part_index & 1) and part.find("#") != -1:
			index += part.find("#")
			break
		index += len(part) + 1
	return line[:index]

# We are parsing list: it is already well formatted.
in_list = False
# Next key:value pair needs comma before it.
need_comma = False

block_start_re = re.compile(r"^\s*([a-zA-Z0-9._+-]+)\s*{")
block_end_re = re.compile(r"^\s*}")
list_start_re = re.compile(r"^\s*([a-zA-Z0-9._+-]+)\s*=\s*\[[^\]]+\s*$")
list_end_re = re.compile(r"^\s*]")
ident_re = re.compile(r"^\s*([a-zA-Z0-9._+-]+)\s*=\s*(.+)\s*")

def convert_to_json(line):
	global in_list, need_comma
	result = []
	match = block_start_re.match(line)
	# key {   -->   [,] "key" : {
	if match is not None:
		if need_comma:
			need_comma = False
			result.append(",")
		result.append("\"" + match.group(1) + "\": {")
		return result
	match = block_end_re.match(line)
	# }  -->  }
	if match is not None:
		need_comma = True
		result.append("}")
		return result
	match = list_end_re.match(line)
	# ]  -->  ]
	if match is not None:
		in_list = False
		result.append("]")
		return result

	if in_list:
		result.append(line.strip())
		return result

	# key = [...  -->  [,] "key": [...
	match = list_start_re.match(line)
	if match is not None:
		in_list = True
	match = ident_re.match(line)
	# key = value  -->  [,] "key": value
	if match is not None:
		if need_comma:
			result.append(",")
		need_comma = True
		result.append("\"" + match.group(1) + "\": " + match.group(2).strip())
		return result
	return None

if len(sys.argv) < 3:
	print "Usage: lvm_parser.py config_file vg_name"
	exit(1)

json_lines = []
for lineno, line in enumerate(open(sys.argv[1])):
	line = strip_comment(line)
	if len(line) == 0 or line.isspace():
		continue
	json_line = convert_to_json(line)
	if json_line is None:
		sys.stderr.write("Unexpected line: %d '%s'\n" % (lineno, line))
	else:
		json_lines.extend(json_line)

s = "{" + "".join(json_lines) + "}"
d = json.loads(s)

vg = d[sys.argv[2]]
print "{} {} {}".format(sys.argv[2], vg["extent_size"], vg.get("status"))

# e.g. {"pv0": "/dev/sda1"}
pv_map = {}
for pv, pv_content in vg["physical_volumes"].items():
	pv_map[pv] = pv_content["device"]

if "logical_volumes" not in vg:
	# Empty VG is valid
	exit(0)

for lv, lv_content in vg["logical_volumes"].items():
	for i in range(1, lv_content["segment_count"] + 1):
		segment = lv_content["segment" + str(i)]
		stripe_count = int(segment["stripe_count"])
		stripe_size = int(segment["extent_count"]) / stripe_count
		for pv, offset in zip(segment["stripes"][::2], segment["stripes"][1::2]):
			print "{}:{} {} {} {}[{}..{}] {}".format(
				   lv, i,
				   "linear" if stripe_count == 1 else "stripped",
			   	   "last" if i == lv_content["segment_count"] else "",
				   pv_map[pv], offset,
				   int(offset) + stripe_size - 1, lv_content.get("status"))

