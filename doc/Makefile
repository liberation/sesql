# Makefile for GNU make. On FreeBSD, use gmake.

# Copyright (c) Pilot Systems and Libération, 2010-2011

# This file is part of SeSQL.

# SeSQL is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# Foobar is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with SeSQL.  If not, see <http://www.gnu.org/licenses/>.


SOURCES = install.txt datamodel.txt query.txt admin.txt bench.txt

PDF = $(SOURCES:.txt=.pdf)
HTML = $(SOURCES:.txt=.html)
TEX = $(SOURCES:.txt=.tex)

RST2HTML = rst2html
RST2LATEX = rst2latex
RM = rm -f
LATEX = pdflatex
GZIP = gzip

all: pdf html clean

pdf: $(PDF)

html: $(HTML)

clean:
	$(RM) $(TEX) *.aux *.log *.toc *.out

fclean: clean
	$(RM) $(PDF) $(HTML)

.PHONY: all pdf html clean fclean
.SUFFIXES: .tex .txt .pdf .html

%.tex: %.txt
	$(RST2LATEX) $< > $@

%.pdf: %.tex
	$(LATEX) $<
	$(LATEX) $<

%.html: %.txt
	$(RST2HTML) $< > $@
