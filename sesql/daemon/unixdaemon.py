# -*- coding: utf-8 -*-

# Copyright (c) Pilot Systems and Libération, 2010-2011

# This file is part of SeSQL.

# SeSQL is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# SeSQL is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with SeSQL.  If not, see <http://www.gnu.org/licenses/>.

import sys, os, time, signal, os.path
import logging

class LogWrapper(object):
    """File-like object for log files"""
    def __init__(self, callback):
        self.callback = callback
        self.buffer = ""

    def write(self, msg):
        if not msg:
            return
        if (msg[-1] == "\n"):
            self.buffer += msg[:-1]
            self.flush()
        else:
            self.buffer += msg

    def flush(self):
        self.callback(self.buffer)
        self.buffer = ""

    def seek(self):
        pass

def logwrap():
    logger = logging.getLogger("daemon")
    sys.stdout = LogWrapper(logging.info)
    sys.stderr = LogWrapper(logging.error)
    
def daemonize(pidfile, ):
    """Do forks and other nice tricks to ensure the code following
    the call of daemonize will run as a Unix demaon"""

    logwrap()

    try:
        previous_pid = int(file(pidfile).read())
        os.kill(previous_pid, signal.SIGCONT)
        raise RuntimeError("Daemon already running with pid %d" % previous_pid)
    except OSError:
        os.remove(pidfile)
        pass
    except IOError:
        pass

    try:
        pid = os.fork()
        if pid > 0:
            count = 0
            while count < 50:
                count += 1
                time.sleep(0.1)
                try:
                    return int(file(pidfile).read())
                except:
                    pass
            raise RuntimeError("Daemon not started.")
    except OSError, e:
        logging.error("Fork #1 failed: %d (%s)" % (e.errno, e.strerror))
        raise

    # Decouple
    os.chdir('/')
    os.setsid()
    os.umask(0)

    # Do second fork
    try:
        pid = os.fork()
    except OSError, e:
        logging.error("Fork #2 failed: %d (%s)", e.errno, e.strerror)
        sys.exit(1)
        
    if pid > 0:
        # exit from second parent, print new PID before
        logging.info("Daemon PID %d", pid)
        try:
            pidfile = file(pidfile, "w")
            pidfile.write("%d\n" % pid)
            pidfile.close()
        except:
            os.kill(pid, signal.SIGKILL)
            sys.exit(1)
        sys.exit(0)


class UnixServiceManager(object):
    """
    Base class to implement a service manager, subclass it and implement the
    run() method.
    """

    def __init__(self):
        """
        Initialise the manager.
        """
        self.childpid = -1

    def stop(self):
        """
        Stop the child Unix process.
        """
        logging.info("Stopping the service...")
        if self.childpid<1:
            logging.warn("Service not running!")
            return
        try:
            os.kill(self.childpid, signal.SIGTERM)
        except OSError, e:
            logging.error("Kill failed: %d (%s)" % (e.errno, e.strerror))
        try:
            os.waitpid(self.childpid, 0)
        except OSError, e:
            logging.error("Waitpid failed: %d (%s)" % (e.errno, e.strerror))
        self.childpid = -1

    def restart(self):
        """
        Restart the process.
        """
        logging.info("Restarting the service...")
        self.stop()
        self.start()

    def start(self):
        """
        Start the process.
        """
        logging.info("Starting the service...")

        
        pid = os.fork()
            
        if (pid > 0):
            self.childpid = pid
            signal.signal(signal.SIGHUP, self.sighup)
            signal.signal(signal.SIGTERM, self.sigterm)
            while True:
                signal.pause()
        else:
            self.run()
            sys.exit(0)

    def run(self):
        """Run the service, override that"""
        raise "Not implemented"""

class UnixDaemon (UnixServiceManager):
    """
    Base class to implement a deamon, subclass it and implement the run()
    method.

    SIGHUP: restart
    SIGTERM: stop and quit
    """

    def __init__(self, pidfile):
        """
        Initialize the demaon.
        """
        UnixServiceManager.__init__(self)      
        self.pidfile = os.path.realpath(pidfile)

    def start_deamon(self):
        """
        Start the daemon.
        """
        pid = daemonize(self.pidfile)
        if pid is None:
            self.start()

        return pid

    def stop_daemon(self):
        """
        Stop the daemon.
        """
        pid = int(file(self.pidfile).read())
        os.kill(pid, signal.SIGTERM)

    def sighup(self, signalid, stack):
        """
        Handle restarting the service.
        """
        logging.info("SIGHUP received, restarting service")
        self.restart()

    def sigterm(self, signalid, stack):
        """
        Sigterm received
        """
        logging.info("SIGTERM received, stopping service")
        self.stop()
        try:
            os.remove(self.pidfile)
        except:
            pass
        sys.exit(1)

    def logwrap(self):
        """
        Wrap stdout and stderr as log files
        """
        logwrap()
