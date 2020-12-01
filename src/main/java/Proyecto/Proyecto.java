package Proyecto;
import java.io.IOException;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class Proyecto {
	private static String nuevaCarpeta = "";
	private static String nombreArchivo = "";
	private static String contenido = "";
	private static String prope = "";
	private static String permiso = "";
	private static int p = 0;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//Creamos la configuración de acceso al HDFS
				Configuration conf = new Configuration(true);
				conf.set("fs.defaultFS", "hdfs://192.168.0.14:8020/");
				
				System.setProperty("HADOOP_USER_NAME", "hdfs");
				
				try {
					Scanner a = new Scanner(System.in);
					//Crear objeto FileSystem
					FileSystem fs = FileSystem.get(conf);
					
					String home = fs.getHomeDirectory().toString();
					System.out.println("¿Que nombre quieres para tu carpeta?");
					nuevaCarpeta = a.nextLine();
					//En caso de que no exista la carpeta, crear la carpeta.
					if(!fs.exists(new Path(home + "/" + nuevaCarpeta))) {
						fs.mkdirs(new Path(home + "/" + nuevaCarpeta));
					}
					
					System.out.println("¿Que nombre quieres para tu archivo?");
					nombreArchivo = a.nextLine();
					//Si no existe el archivo, hay que crearlo
					Path rutaArchivo = new Path(home + "/" + nuevaCarpeta + "/" + nombreArchivo);
					FSDataOutputStream outputStream = null;
					
					System.out.println("¿Que contendra tu archivo?");
					contenido = a.nextLine();
					
					if(!fs.exists(rutaArchivo)) {
						outputStream = fs.create(rutaArchivo);
						outputStream.writeBytes(contenido);
						outputStream.close();
					}
					
					System.out.println("¿Quien es el propietario del archivo?");
					prope = a.nextLine();
					
					//Tambien podemos modificar el propietario o los permisos del archivo.
					fs.setOwner(rutaArchivo, prope, prope);
					
					System.out.println("¿Deseas modificar permisos? Si=1 No= Cualquier");
					p = a.nextInt();
					if (p==1) {
						System.out.println("¿Que permisos quieres?");
						System.out.println("Ejemplo: -rwxrwxrwx");
						permiso = a.nextLine();
						FsPermission permisos = FsPermission.valueOf(permiso);
						fs.setPermission(rutaArchivo, permisos);
					}
					
					System.out.println("¿Deseas borrar algun archivo? Si=1 No=Cualquier");
					p = a.nextInt();
					if (p==1) {
						System.out.println("Dime la carpeta del archivo a borrar");
						nuevaCarpeta= a.nextLine();
						System.out.println("Dime el archivo a borrar");
						nombreArchivo= a.nextLine();
						//Por ultimo, borraremos el directorio y los archivos.
						fs.delete(new Path(home + "/" + nuevaCarpeta + "/" + nombreArchivo), true);
					
					}
					fs.close();
				
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
	}

}
