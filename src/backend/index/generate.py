#!/usr/bin/python

def decorate(fin, fout):
  i = open(fin)
  o = open(fout, "w")
  for line in i.readlines():
    if not "//" in line and "MASALA" in line:
      target = line[line.find('<')+1:line.find('>')]
      o.write("// {} start\n".format(target))
      t = open(target)
      for linet in t.readlines():
        o.write(linet)
      o.write("// {} end\n".format(target))
    else:
      o.write(line)

decorate("bwtree_template.cpp", "bwtree.cpp")
decorate("bwtree_template.h", "bwtree.h")