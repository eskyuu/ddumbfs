SUBDIRS = src doc examples man scripts rpmbuild debbuild
EXTRA_DIST = accounting.py docs/Makefile docs/source test/ddfstest.sh test/pjd-fstest-20080816

dist-hook:
	python accounting.py ${distdir}
	
sphinx:
	cd docs ; make html man
	cp ./docs/build/man/* man 
	cp ./docs/build/html/man/* man
     
upload:
	lftp -f upload.lftp

rpms srcrpm: dist
	$(MAKE) -C rpmbuild $(AM_MAKEFLAGS) $@
	rpmarch=`rpm --showrc | grep ^build\ arch | sed 's/\(.*: \)\(.*\)/\2/'`; \
	test -z $$rpmarch || ( mv $$rpmarch/* .; rm -rf $$rpmarch )
	-rm -rf $(distdir)

deb: dist
	$(MAKE) -C debbuild $(AM_MAKEFLAGS) $@
	

.PHONY: srcrpm rpms deb
