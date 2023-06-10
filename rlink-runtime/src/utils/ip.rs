use std::net::IpAddr;

pub fn get_service_ip() -> std::io::Result<std::net::IpAddr> {
    let ip_addrs = match get_hostname() {
        Ok(hn) => {
            let ip_addrs = get_ip_addrs(hn.as_str()).unwrap_or(vec![]);
            let ip_addrs: Vec<IpAddr> = ip_addrs
                .iter()
                .filter(|x| !x.is_loopback())
                .map(|x| x.clone())
                .collect();
            ip_addrs
        }
        Err(_) => vec![],
    };

    if ip_addrs.len() == 1 {
        return Ok(ip_addrs[0].clone());
    }

    get_local_ip()
}

pub fn get_hostname() -> std::io::Result<String> {
    let hn = hostname::get()?;
    match hn.into_string() {
        Ok(str) => Ok(str),
        Err(_os_str) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "get hostname error",
        )),
    }
}

pub fn get_ip_addrs(hostname: &str) -> std::io::Result<Vec<std::net::IpAddr>> {
    dns_lookup::lookup_host(hostname)
}

/// get the local ip address, return an `Option<String>`. when it fail, return `None`.
pub fn get_local_ip() -> std::io::Result<std::net::IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;

    socket.connect("8.8.8.8:80")?;

    socket.local_addr().map(|socket_addr| socket_addr.ip())
}
