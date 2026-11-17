import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Scanner;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseLab implements AutoCloseable {
    private final Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
    private final Admin admin = connection.getAdmin();
    private static final String NAMESPACE = "lab3_java";

    public HBaseLab() throws IOException {
        if (!Arrays.asList(admin.listNamespaces()).contains(NAMESPACE)) {
            admin.createNamespace(NamespaceDescriptor.create(NAMESPACE).build());
        }
    }

    private TableName name(String table) {
        if (table.contains(":")) throw new IllegalArgumentException("Use a table in the lab3_java namespace");
        return TableName.valueOf(NAMESPACE, table);
    }

    public void createTable(String tableName, String[] fields) throws IOException {
        TableName table = name(tableName);
        if (admin.tableExists(table)) {
            if (admin.isTableEnabled(table)) admin.disableTable(table);
            admin.deleteTable(table);
        }
        TableDescriptorBuilder descriptor = TableDescriptorBuilder.newBuilder(table);
        LinkedHashSet<String> families = new LinkedHashSet<>();
        for (String field : fields) families.add(field.split(":", 2)[0]);
        for (String family : families) descriptor.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
        admin.createTable(descriptor.build());
    }

    public void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        if (fields.length != values.length) throw new IllegalArgumentException("Fields and values must have equal lengths");
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < fields.length; i++) {
            String[] column = fields[i].split(":", 2);
            put.addColumn(Bytes.toBytes(column[0]), Bytes.toBytes(column.length == 2 ? column[1] : "value"), Bytes.toBytes(values[i]));
        }
        try (Table table = connection.getTable(name(tableName))) { table.put(put); }
    }

    public void listTables() throws IOException {
        for (TableName table : admin.listTableNames()) System.out.println(table.getNameAsString());
    }

    public void scanTable(String tableName) throws IOException {
        try (Table table = connection.getTable(name(tableName)); ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result row : scanner) {
                for (Cell cell : row.rawCells()) {
                    System.out.println(Bytes.toString(row.getRow()) + " " + Bytes.toString(CellUtil.cloneFamily(cell))
                            + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "=" + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        }
    }

    public void scanColumn(String tableName, String column) throws IOException {
        String[] parts = column.split(":", 2);
        byte[] family = Bytes.toBytes(parts[0]);
        try (Table table = connection.getTable(name(tableName)); ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result row : scanner) {
                boolean found = false;
                for (Cell cell : row.rawCells()) {
                    if (CellUtil.matchingFamily(cell, family) && (parts.length == 1 || CellUtil.matchingQualifier(cell, Bytes.toBytes(parts[1])))) {
                        System.out.println(Bytes.toString(row.getRow()) + " " + parts[0] + ":"
                                + Bytes.toString(CellUtil.cloneQualifier(cell)) + "=" + Bytes.toString(CellUtil.cloneValue(cell)));
                        found = true;
                    }
                }
                if (!found) System.out.println(Bytes.toString(row.getRow()) + " " + column + "=null");
            }
        }
    }

    public void modifyData(String tableName, String row, String column) throws IOException {
        System.out.println("Enter new value:");
        modifyData(tableName, row, column, new Scanner(System.in).nextLine());
    }

    public void modifyData(String tableName, String row, String column, String value) throws IOException {
        addRecord(tableName, row, new String[]{column}, new String[]{value});
    }

    public void deleteRow(String tableName, String row) throws IOException {
        try (Table table = connection.getTable(name(tableName))) { table.delete(new Delete(Bytes.toBytes(row))); }
    }

    public void addFamily(String tableName, String family) throws IOException {
        admin.addColumnFamily(name(tableName), ColumnFamilyDescriptorBuilder.of(family));
    }

    public void deleteFamily(String tableName, String family) throws IOException {
        admin.deleteColumnFamily(name(tableName), Bytes.toBytes(family));
    }

    public void deleteColumn(String tableName, String row, String column) throws IOException {
        String[] parts = column.split(":", 2);
        try (Table table = connection.getTable(name(tableName))) {
            table.delete(new Delete(Bytes.toBytes(row)).addColumns(Bytes.toBytes(parts[0]), Bytes.toBytes(parts[1])));
        }
    }

    public long countRows(String tableName) throws IOException {
        long count = 0;
        try (Table table = connection.getTable(name(tableName)); ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result ignored : scanner) count++;
        }
        System.out.println(tableName + " rows=" + count);
        return count;
    }

    public void clearTable(String tableName) throws IOException {
        admin.disableTable(name(tableName));
        admin.truncateTable(name(tableName), true);
    }

    public void close() throws IOException { admin.close(); connection.close(); }

    public static void main(String[] args) throws Exception {
        try (HBaseLab lab = new HBaseLab()) {
            lab.createTable("Student", new String[]{"info", "Score"});
            String[] studentFields = {"info:S_No", "info:S_Name", "info:S_Sex", "info:S_Age"};
            lab.addRecord("Student", "Zhangsan", studentFields, new String[]{"2015001", "Zhangsan", "male", "23"});
            lab.addRecord("Student", "Mary", studentFields, new String[]{"2015003", "Mary", "female", "22"});
            lab.addRecord("Student", "Lisi", studentFields, new String[]{"2015003", "Lisi", "male", "24"});
            lab.addRecord("Student", "Zhangsan", new String[]{"Score:Math", "Score:English"}, new String[]{"86", "69"});
            lab.createTable("Course", new String[]{"info"});
            String[] courseFields = {"info:C_No", "info:C_Name", "info:C_Credit"};
            lab.addRecord("Course", "123001", courseFields, new String[]{"123001", "Math", "2.0"});
            lab.addRecord("Course", "123002", courseFields, new String[]{"123002", "Computer Science", "5.0"});
            lab.addRecord("Course", "123003", courseFields, new String[]{"123003", "English", "3.0"});
            lab.createTable("SC", new String[]{"info"});
            String[][] choices = {{"2015001", "123001", "86"}, {"2015001", "123003", "69"}, {"2015002", "123002", "77"},
                    {"2015002", "123003", "99"}, {"2015003", "123001", "98"}, {"2015003", "123002", "95"}};
            for (String[] row : choices) lab.addRecord("SC", row[0] + "_" + row[1], new String[]{"info:SC_Sno", "info:SC_Cno", "info:SC_Score"}, row);
            lab.listTables();
            lab.scanTable("Student"); lab.scanTable("Course"); lab.scanTable("SC");
            lab.countRows("Student"); lab.countRows("Course"); lab.countRows("SC");
            lab.scanColumn("Student", "Score"); lab.scanColumn("Student", "Score:Math");
            lab.createTable("Ops", new String[]{"info"});
            lab.addRecord("Ops", "row1", new String[]{"info:value"}, new String[]{"old"});
            lab.createTable("Ops", new String[]{"info"});
            lab.countRows("Ops");
            lab.addFamily("Ops", "extra");
            lab.addRecord("Ops", "row1", new String[]{"info:value", "extra:note"}, new String[]{"old", "temporary"});
            lab.modifyData("Ops", "row1", "info:value", "new");
            lab.scanTable("Ops");
            lab.deleteColumn("Ops", "row1", "extra:note");
            lab.deleteFamily("Ops", "extra");
            lab.addRecord("Ops", "row2", new String[]{"info:value"}, new String[]{"second"});
            lab.deleteRow("Ops", "row2"); lab.countRows("Ops");
            lab.clearTable("Ops"); lab.countRows("Ops");
            System.out.println("HBase Java operations completed");
        }
    }
}
