import sys, os, signal, yaml
from struct import pack, unpack

from ccmlib import common
from ccmlib.cmds import command, cluster_cmds, node_cmds

from optparse import OptionParser

def get_command(kind, cmd):
    cmd_name = kind.lower().capitalize() + cmd.lower().capitalize() + "Cmd"
    try:
        klass = (cluster_cmds if kind.lower() == 'cluster' else node_cmds).__dict__[cmd_name]
    except KeyError:
        return None
    if not issubclass(klass, command.Cmd):
        return None
    return klass()

def _signal_handler(signal, frame):
    """
    Handle the signal given in the first argument, exiting gracefully
    """
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    if sys.platform != "win32":
        signal.signal(signal.SIGHUP, _signal_handler)

    if sys.platform == "win32":
        # disable CRLF
        import msvcrt
        msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
    else:
        # close fd's inherited from the ruby parent
        import resource
        maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
        if maxfd == resource.RLIM_INFINITY:
            maxfd = 65536

        for fd in range(3, maxfd):
            try:
                os.close(fd)
            except:
                pass

    sys.stderr.write('\x00');

    while True:
        size = unpack('H', sys.stdin.read(2))[0]
        args = yaml.safe_load(sys.stdin.read(size))
        args = [arg.encode('utf-8') for arg in args]
        arg1 = args[0].lower()

        if arg1 in cluster_cmds.commands():
            kind = 'cluster'
            cmd = arg1
            cmd_args = args[1:]
        else:
            kind = 'node'
            node = arg1
            cmd  = args[1]
            cmd_args = [node] + args[2:]

        cmd    = get_command(kind, cmd)
        parser = cmd.get_parser()

        (options, args) = parser.parse_args(cmd_args)

        cmd.validate(parser, options, args)

        cmd.run()
        sys.stderr.write('\x01')
