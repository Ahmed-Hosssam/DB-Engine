import java.io.*;

public class DiskHandler implements Serializable{
    public void serializeObject(Object o, String path) throws IOException {
        FileOutputStream fileOut = new FileOutputStream(path);
        ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
        objectOut.writeObject(o);
        objectOut.close();
        fileOut.close();
    }

    public Object deserializeObject(String path) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(path);
        ObjectInputStream objectIn = new ObjectInputStream(fileIn);
        Object o = objectIn.readObject();
        objectIn.close();
        fileIn.close();
        return o;
    }

    /**
     * deletes the file from the disk
     *
     * @param path the path of the file to be deleted.
     */
    public boolean delete(String path) {
        File f = new File(path);
        return f.delete();
    }
}
