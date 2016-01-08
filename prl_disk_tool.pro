CONFIG += qt

QT = core xml
LIBS += -lguestfs -Wl,-Bstatic -lboost_program_options -Wl,-Bdynamic

# Application name string
DEFINES += APP_NAME_STR=\\\"$${APP_NAME}\\\"

HEADERS += GuestFSWrapper.h \
           DiskLock.h \
           Command.h \
           ImageInfo.h \
           Util.h \
           Abort.h \
           Expected.h \
           ProgramOptions.h \
           StringTable.h \
           Errors.h

SOURCES += main.cpp \
           GuestFSWrapper.cpp \
           DiskLock.cpp \
           Command.cpp \
           CommandVm.cpp \
           CommandCt.cpp \
           ImageInfo.cpp \
           Util.cpp \
           Abort.cpp \
           ProgramOptions.cpp \
           StringTable.cpp


target.path = /usr/sbin/
INSTALLS += target

LINKNAME = prl-disk-tool

link.commands = $(SYMLINK) $$TARGET $(INSTALL_ROOT)$${target.path}$$LINKNAME
link.uninstall = -$(DEL_FILE) $${target.path}$$LINKNAME
link.path = /usr/sbin
INSTALLS += link
