package mydb.transport;

public class Packager {
   Transporter transporter;
   Encoder encoder;
   public Packager(Transporter transpoter, Encoder encoder) {
      this.transporter = transpoter;
      this.encoder = encoder;
   }

   public void send(Package pkg) throws Exception {
      byte[] data = encoder.encode(pkg);
      transporter.send(data);
   }

   public Package receive() throws Exception {
      byte[] data = transporter.receive();
      return encoder.decode(data);
   }

   public void close() throws Exception {
      transporter.close();
   }



}
