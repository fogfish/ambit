sysctl -w kern.ipc.maxsockbuf=4194304
sysctl -w kern.ipc.somaxconn=2048
sysctl -w kern.ipc.nmbclusters=2048
sysctl -w net.inet.tcp.rfc1323=1
sysctl -w net.inet.tcp.win_scale_factor=4
sysctl -w net.inet.tcp.sockthreshold=16
sysctl -w net.inet.tcp.sendspace=1042560
sysctl -w net.inet.tcp.recvspace=1042560
sysctl -w net.inet.tcp.mssdflt=1448
sysctl -w net.inet.tcp.v6mssdflt=1428
sysctl -w net.inet.tcp.msl=15000
sysctl -w net.inet.tcp.always_keepalive=0
sysctl -w net.inet.tcp.delayed_ack=3
sysctl -w net.inet.tcp.slowstart_flightsize=20
sysctl -w net.inet.tcp.local_slowstart_flightsize=20
sysctl -w net.inet.tcp.blackhole=2
sysctl -w net.inet.udp.blackhole=1
sysctl -w net.inet.icmp.icmplim=50

# sudo launchctl limit maxfiles 16384 16384
