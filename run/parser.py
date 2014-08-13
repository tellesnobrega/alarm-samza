import re
import sys

def parse_line(base_time, second_total, message):
    hour = base_time[0]
    minutes = base_time[1]
    seconds = base_time[2]

    line = hour +";"+minutes+";"+seconds+";"+ str(message) + ";" +  str(second_total)
    return line

def _add(second_total):
    second_total += 1
    return second_total

def _write(f, line):
    f.write(line+"\n")

def main(args):
  second_total_ack = 0
  second_total_event = 0
  file_name = str(args[0])
  out_file = str(args[1])
  f = open(file_name, 'r')
  out = open(out_file, 'w')
  _write(out, "hour;minute;second;event;total")
  base_time="-1"
  for line in f:
      if 'AckSent' or 'EventSent' in line:
          line_split = line.strip().split(";")
          if(base_time == "-1"):
              base_time = line_split[0].split(":")
          new_base_time = line_split[0].split(":")
          if not (new_base_time[0] == base_time[0] and new_base_time[1] == base_time[1] and new_base_time[2] == base_time[2]):
              if not (second_total_ack == 0):
                  to_write = parse_line(base_time,second_total_ack, "AckSent")
                  _write(out, to_write)
              second_total_ack = 0
              if not (second_total_event == 0):
                  to_write = parse_line(base_time,second_total_event, "EventSent")
                  _write(out, to_write)
              second_total_event = 0
              if "AckSent" in line:
                  second_total_ack = _add(second_total_ack)
              elif "EventSent" in line:
                   second_total_event = _add(second_total_event)
              base_time = new_base_time
          else:
              if "AckSent" in line:
                  second_total_ack = _add(second_total_ack)
              elif "EventSent" in line:
                  second_total_event = _add(second_total_event)
 
  if not (second_total_ack == 0):
      to_write = parse_line(base_time, second_total_ack, "AckSent")
      _write(out, to_write)
  if not (second_total_event == 0):
      to_write = parse_line(base_time, second_total_event, "EventSent")
      _write(out, to_write)

if __name__ == "__main__":
  main(sys.argv[1:])
