# top level makefile for tardis

.PHONY: all

all: README.rst

README.rst: README.md
	pandoc $< -o $@
