use colored::Colorize;
use redis::{ Commands, streams::StreamMaxlen };
use socket2::{ Domain, Protocol, Socket, Type };
use std::net::{ Ipv4Addr, SocketAddrV4, UdpSocket };
use std::str::FromStr;

fn be_u32(data: &[u8], idx:usize) -> u32
{
  u32::from_be_bytes(data[idx..idx+4].try_into().unwrap())
}

fn be_u16(data: &[u8], idx:usize) -> u16
{
  u16::from_be_bytes(data[idx..idx+2].try_into().unwrap())
}

fn be_i16(data: &[u8], idx:usize) -> i16
{
  i16::from_be_bytes(data[idx..idx+2].try_into().unwrap())
}

fn main()
{
  let client = redis::Client::open("redis://127.0.0.1").unwrap();

  let mut redis = client.get_connection();

  match redis
  {
    Ok(_) =>  println!("{}", "\nConnected to Redis".white()),
    Err(_) => println!("{}", "\nNo Redis connection".magenta())
  }

  let sock2 = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)).unwrap();

  let _ = sock2.set_reuse_address(true);

  let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 4357);

  let _ = sock2.bind(&addr.into());

  let group = Ipv4Addr::from_str("239.128.1.1").unwrap();

  let _ = sock2.join_multicast_v4(&group, &Ipv4Addr::UNSPECIFIED);

  let sock: UdpSocket = sock2.into();

  println!("{}", "\nListening to Multicast ...".white());

  let mut buf = [0u8; 9999];

  loop
  {
    let (len, _) = sock.recv_from(&mut buf).unwrap();

    if len == buf.len() { println!("{}", "\nMax data received".bright_red()); }

    println!("{}", format!("\nReceived {len} bytes").white());

    if len < 32
    {
      println!("{}", "\nHeader too short".bright_red());
      continue;
    }

    let ip_ver  = buf[ 0] as f32 + buf[ 1] as f32 / 10.0;
    let mc_ver  = buf[20] as f32 + buf[21] as f32 / 10.0;
    let seq_num = be_u32(&buf, 24);

    if len == 32+28
    {
      let hb = &buf[32..];

      let typecode    = hb[0];
      let seconds    = be_u32(&hb, 4);
      let edm_seq    = be_u32(&hb, 8);
      let evt_seq    = be_u32(&hb, 12);
      let evt_num    = be_u32(&hb, 16);
      let hb_seq     = be_u32(&hb, 20);
      let count      = be_u32(&hb, 24);

      println!("{}", format!("HB  ip_ver: {ip_ver:.1}   mc_ver: {mc_ver:.1}   seq_num: {seq_num}   typecode: {typecode}   seconds: {seconds}").green());
      println!("{}", format!("    edm_seq: {edm_seq}    evt_seq: {evt_seq}    evt_num: {evt_num}   hb_seq: {hb_seq:4}   count: {count:3}").green());
    }
    else if len == 32+72
    {
      let mclr = &buf[32..];

      let typecode   = mclr[0];
      let daemon_id = be_u32(&mclr, 64);
      let edm_seq   = be_u32(&mclr, 68);

      println!("{}", format!("MCLR  ip_ver: {ip_ver:.1}   mc_ver: {mc_ver:.1}   seq_num: {seq_num}   typecode: {typecode}   daemon_id: {daemon_id}   edm_seq: {edm_seq}").magenta());
    }
    else
    {
      let hdr = &buf[32..];

      let typecode  = hdr[0];
      let count     = hdr[1];
      let version   = hdr[2];
      let edm_seq  = be_u32(&hdr, 4);

      println!("{}", format!("EDP  ip_ver: {ip_ver:.1}   mc_ver: {mc_ver:.1}   seq_num: {seq_num}   typecode: {typecode}   count: {count}   version: {version}   edm_seq: {edm_seq}").green());

      let mut num_edp = (len - 40) / 192;

      if num_edp != count as usize
      {
        println!("{}", format!("Calculated # of edps {num_edp} does not match header count {count}").bright_red());
      }

      let mut edp = &hdr[8..];

      while num_edp > 0
      {
        let typecode  = edp[0];   let priority  = edp[1];   let trunk   = edp[2];   let node    = edp[3];   let ssn     = edp[4];
        let bs        = edp[5];   let erp_type  = edp[6];   let dig_edp = edp[7];   let broken  = edp[8];   let unused  = edp[9];

        let status  = be_u16(&edp, 10);   let handler = be_u16(&edp, 12);   let alarm_list  = be_i16(&edp, 14);

        let dev_index = be_u32(&edp, 16);   let dev_class = be_u32(&edp, 20);
        let dev_type  = be_u32(&edp, 24);   let seconds   = be_u32(&edp, 28);
        let seq_num   = be_u32(&edp, 32);   let sound_id  = be_u32(&edp, 36);
        let speech_id = be_u32(&edp, 40);   let raw_dat   = be_u32(&edp, 44);

        let name      = str::from_utf8(&edp[48..64]).unwrap().trim_end_matches('\0').trim_end();
        let full_name = str::from_utf8(&edp[64..128]).unwrap().trim_end_matches('\0').trim_end();
        let text      = str::from_utf8(&edp[128..192]).unwrap().trim_end_matches('\0').trim_end();

        let bypass    = ((status & 1) == 0)   as u8;
        let alarm     = ((status >>  1) & 1)  as u8;
        let trigger   = ((status >>  2) & 1)  as u8;
        let inhibit   = ((status >>  3) & 1)  as u8;
        let reserved  = ((status >>  4) & 1)  as u8;
        let q_code    = ((status >>  5) & 3)  as u8;
        let dig_st    = ((status >>  7) & 1)  as u8;
        let k_code    = ((status >>  8) & 7)  as u8;
        let low       = ((status >> 11) & 1)  as u8;
        let high      = ((status >> 12) & 1)  as u8;
        let exception = ((status >> 13) & 1)  as u8;
        let logging   = ((status >> 14) & 1)  as u8;
        let display   = ((status >> 15) & 1)  as u8;

        let is_digital  = dig_edp != 0 || dig_st != 0;
        let is_mismatch = dig_edp != dig_st;
        let is_low_high = low != 0 && high != 0;

        let line1 =
        {
          let txt = format!("name: {name}   seconds: {seconds}   seq_num: {seq_num}   full_name: {full_name}   text: {text}");
          if is_digital { txt.cyan() } else { txt.yellow() }
        };

        //  typecode: 123   priority: 123   trunk: 12345   node: 123456789   ssn: 12345678   dev_type: 1234567
        //  broken: 12345   unused: 12345   handler: 123   alarm_list: 123   erp_type: 123   dev_index: 123456
        //  dev_class: 12   bs: 123456789   sound_id: 12   speech_id: 1234   dig_edp: 1234   raw_dat: 0x12345678

        let line2 =
        {
          let txt = format!("typecode: {typecode:3}   priority: {priority:3}   trunk: {trunk:5}   node: {node:9}   ssn: {ssn:8}   dev_type: {dev_type:7}");
          if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line3 =
        {
          let txt = format!("broken: {broken:5}   unused: {unused:5}   handler: {handler:3}   alarm_list: {alarm_list:3}   erp_type: {erp_type:3}   dev_index: {dev_index:6}");
          if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line4a =
        {
          let txt = format!("dev_class: {dev_class:2}   bs: {bs:9}   sound_id: {sound_id:2}   speech_id: {speech_id:4}");
          if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line4b =
        {
          let txt = format!("   dig_edp: {dig_edp:4}");
          if is_mismatch { txt.bright_red() } else if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line4c =
        {
          let txt = format!("   raw_dat: {raw_dat:#8x}");
          if is_digital { txt.cyan() } else { txt.yellow() }
        };

        //  status: 0x1234   bypass: 12   alarm: 1   trigger: 1   inhibit: 123   reserved: 1   q_code: 12
        //  dig_st:   1234   k_code: 12   low: 123   high: 1234   exception: 1   logging: 12   display: 1

        let line5a =
        {
          let txt = format!("status: {status:#06x}   ");
          if status == 0 { txt.bright_red() } else if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line5b =
        {
          let txt = format!("bypass: {bypass:2}   ");
          if bypass != 0 { txt.bright_blue() } else if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line5c =
        {
          let txt = format!("alarm: {alarm:1}   ");
          if alarm != 0 { txt.magenta() } else if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line5d =
        {
          let txt = format!("trigger: {trigger:1}   inhibit: {inhibit:3}   reserved: {reserved:1}   q_code: {q_code:2}");
          if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line6a =
        {
          let txt = format!("dig_st: {dig_st:6}   ");
          if is_mismatch { txt.bright_red() } else if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line6b =
        {
          let txt = format!("k_code: {k_code:2}   ");
          if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line6c =
        {
          let txt = format!("low: {low:3}   ");
          if is_low_high { txt.bright_red() } else if low != 0 { txt.magenta() } else if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line6d =
        {
          let txt = format!("high: {high:4}   ");
          if is_low_high { txt.bright_red() } else if high != 0 { txt.magenta() } else if is_digital { txt.cyan() } else { txt.yellow() }
        };
        let line6e =
        {
          let txt = format!("exception: {exception:1}   logging: {logging:2}   display: {display:1}");
          if is_digital { txt.cyan() } else { txt.yellow() }
        };

        println!();
        println!("{line1}");
        println!("{line2}");
        println!("{line3}");
        println!("{line4a}{line4b}{line4c}");
        println!("{line5a}{line5b}{line5c}{line5d}");
        println!("{line6a}{line6b}{line6c}{line6d}{line6e}");

        let source   = if dig_st == 0 { "ANALOG" } else { "DIGITAL"};
        let severity = if alarm == 0 { "NO_ALARM" } else if priority < 10 { "MINOR" } else { "MAJOR" };
        let detail   = if bypass == 0
        {
          if alarm == 0
          {
            if dig_st == 0 { "ANALOG" } else { "DIGITAL" }
          }
          else
          {
            if dig_st == 0
            {
              if low != 0 && high == 0 { "LOW" }
              else if low == 0 && high != 0 { "HIGH" }
              else { "ANALOG" }
            }
            else { &raw_dat.to_string() }
          }
        }
        else { "BYPASS" };

        if redis.is_ok()
        {
          let fields =
          [
            ("device", name),       ("source", source), ("timestamp", &seconds.to_string()),
            ("severity", severity), ("detail", detail), ("message", &text.to_string())
          ];
          let cxn = redis.as_mut().unwrap();
          let _: Result<(), _> = cxn.xadd_maxlen("acorn:alarms", StreamMaxlen::Approx(9999), "*", &fields);
        }

        edp = &edp[192..];
        num_edp -= 1;
      }
    }
  }
}
