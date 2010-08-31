package messandra;
import java.util.List;
import java.io.UnsupportedEncodingException;
import java.io.IOException;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;



public class Messandra {



public static final String UTF8 = "UTF8";
   public static void main(String[] args)
    throws TException, IOException, InvalidRequestException, UnavailableException,UnsupportedEncodingException, TimedOutException, NotFoundException
    //throws TException, InvalidRequestException, UnavailableException,UnsupportedEncodingException,NotFoundException
     {
    //Make a connection to the database
    TTransport transport = new TSocket("localhost", 9160);
    TProtocol protocol = new TBinaryProtocol (transport);
    //THe client takes care of the communocation witht the database server
    Cassandra.Client client = new Cassandra.Client(protocol);

    transport.open();

   
  String keyspace = "Keyspace1";
   String ColumnFamily = "Standard2";
   String KeyUserid1 = "1";
   String KeyUserid2 = "2";

   //Use longtimestamp to avoid data inconsistency
    long timestamp = System.currentTimeMillis();

 
    //Create the message management system users and their profilce data
    ColumnPath colPathID = new ColumnPath(ColumnFamily);
   colPathID.setColumn("UserID".getBytes(UTF8));
   client.insert(keyspace, KeyUserid1, colPathID, "Locos".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);

   
    ColumnPath colPathName = new ColumnPath(ColumnFamily);
    colPathName.setColumn("FullName".getBytes(UTF8));
    client.insert(keyspace, KeyUserid1, colPathName, "Veiko Muronga".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);


  ColumnPath colPathAddr = new ColumnPath(ColumnFamily);
    colPathAddr.setColumn("Adress".getBytes(UTF8));
    client.insert(keyspace, KeyUserid1, colPathAddr, "Windhoek Namibia".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);

    //Create the message management system users
    ColumnPath colPathID2 = new ColumnPath(ColumnFamily);
   colPathID2.setColumn("UserID".getBytes(UTF8));
   client.insert(keyspace, KeyUserid2, colPathID2, "AlbertS".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);

    //Enter profile data for user lcoosbadboy
    ColumnPath colPathName2 = new ColumnPath(ColumnFamily);
    colPathName2.setColumn("FullName".getBytes(UTF8));
    client.insert(keyspace, KeyUserid2, colPathName2, "Albert Sibeya".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);


  ColumnPath colPathAddr2 = new ColumnPath(ColumnFamily);
    colPathAddr2.setColumn("Adress".getBytes(UTF8));
    client.insert(keyspace, KeyUserid2, colPathAddr2, "Gijima AST".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);

    //Create the inbox columns
   ColumnPath colPathInbox1a = new ColumnPath(ColumnFamily);
    colPathInbox1a.setColumn("toLocos".getBytes(UTF8));
    client.insert(keyspace, KeyUserid1, colPathInbox1a, "How is everything from your side?, from Albert".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);

    ColumnPath colPathInbox1b = new ColumnPath(ColumnFamily);
    colPathInbox1b.setColumn("toLocos1".getBytes(UTF8));
    client.insert(keyspace, KeyUserid1, colPathInbox1b, "I would like for us to meet up?, from Albert".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);

    ColumnPath colPathInbox2 = new ColumnPath(ColumnFamily);
    colPathInbox2.setColumn("toAlbert".getBytes(UTF8));
    client.insert(keyspace, KeyUserid2, colPathInbox2, "When can we meet up?, from Locos".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);
    
    ColumnPath colPathOutbox1 = new ColumnPath(ColumnFamily);
    colPathOutbox1.setColumn("fromLocos".getBytes(UTF8));
    client.insert(keyspace, KeyUserid1, colPathOutbox1, "toAlbert, When can we meet up?".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);

   /* Search and delete messages from colPathInbox1a column. If you do not want the messages to be deleted, simply
    comment out the command below*/
    client.remove(keyspace, "1", colPathInbox1a, timestamp, ConsistencyLevel.ONE);

    /*Search and insert messages into column colPathInboxa, this function does not append but it replace the content*/
    client.insert(keyspace, "1" , colPathInbox1a, "Did you get my last message, from Albert".getBytes(UTF8), timestamp, ConsistencyLevel.ONE);
    
    //View Incoming messages to user locos by using the slice range structure
  SlicePredicate predicate = new SlicePredicate();
  SliceRange sliceRange = new SliceRange();
  sliceRange.setStart("toLocos".getBytes());
  sliceRange.setFinish("toLocos1".getBytes());
  predicate.setSlice_range(sliceRange);

  System.out.println("");
  ColumnParent parent = new ColumnParent(ColumnFamily);
  List<ColumnOrSuperColumn> results = client.get_slice(keyspace, "1", parent, predicate, ConsistencyLevel.ONE);
  for (ColumnOrSuperColumn result : results){
  Column column = result.column;
  System.out.println(new String(column.name, UTF8)+ ":" + new String(column.value, UTF8));
  }


    //View outgoing messages from user locos

    System.out.println(" ");
    Column col = client.get(keyspace, "1", colPathOutbox1 , ConsistencyLevel.ONE).getColumn();
    System.out.println( new String(col.name, UTF8) +  ":"  + new String (col.value, UTF8));

    transport.close();
    }

}
