open Core
open Async

(* The list of implementations supported by the server.  The server state is simply a
   counter used for allocating unique ids. *)
let implementations =
  [ Rpc.Rpc.implement Rpc_intf.get_unique_id (fun ctr () ->
      printf ".%!";
      incr ctr;
      return !ctr)
  ; Rpc.Rpc.implement Rpc_intf.set_id_counter (fun ctr i ->
      printf "!%!";
      if i = 0 then failwith "Can't set counter back to zero";
      return (ctr := i))
  ; Rpc.Pipe_rpc.implement Rpc_intf.counter_values (fun ctr () ->
      let r, w = Pipe.create () in
      let last_value = ref !ctr in
      let send () =
        last_value := !ctr;
        Pipe.write w !ctr
      in
      don't_wait_for (send ());
      Clock.every' ~stop:(Pipe.closed w) (sec 0.1) (fun () ->
        if !last_value <> !ctr then send () else return ());
      return (Ok r))
  ]
;;

let main ~port =
  Stdio.printf "Running on port %d\n%!" port;
  let counter = ref 0 in
  let log_unknown _counter ~rpc_tag ~version =
      Stdio.printf "Unknown rpc: %s %d\n%!" rpc_tag version;
      `Close_connection
  in
  let implementations =
    Rpc.Implementations.create ~implementations ~on_unknown_rpc:(`Call log_unknown)
  in
  let log_error address exn =
    Stdio.printf "Error %s: %s\n%!" (Socket.Address.to_string address) (Exn.to_string exn)
  in
  let log_handshake_error exn =
      Stdio.printf "Error handshake: %s\n%!" (Exn.to_string exn);
      Deferred.unit
  in
  match implementations with
  | Error (`Duplicate_implementations (_ : Rpc.Description.t list)) -> assert false
  | Ok implementations ->
    let server =
      Tcp.Server.create
        (Tcp.Where_to_listen.of_port port)
        ~on_handler_error:(`Call log_error)
        (fun (_ : Socket.Address.Inet.t) reader writer ->
          let heartbeat_config = Rpc.Connection.Heartbeat_config.create ~timeout:(Time_ns.Span.of_sec 5.) () in
           Rpc.Connection.server_with_close
             reader
             writer
             ~implementations
             ~heartbeat_config
             ~connection_state:(fun (_ : Rpc.Connection.t) -> counter)
             ~on_handshake_error:(`Call log_handshake_error))
    in
    ignore (server : (Socket.Address.Inet.t, int) Tcp.Server.t Deferred.t);
    never ()
;;

type t = int [@@deriving bin_io]
type u = Int64.t [@@deriving bin_io]
type v = t [@@deriving bin_io]
type w = {t: int} [@@deriving bin_io]

let () =
  Command.async
    ~summary:"A trivial Async-RPC server"
    (let%map_open.Command port =
       flag "-port" ~doc:" Port to listen on" (optional_with_default 8080 int)
     in
     fun () -> main ~port)
  |> Command.run
;;
