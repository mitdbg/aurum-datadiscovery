package core;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;

public class Catalog implements Serializable {

	private HashMap<Integer, TaskPackage> tasks;
	
	public Catalog() {
		this.tasks = new HashMap<Integer, TaskPackage>();
	}
	
	public void addTask(int taskId, TaskPackage task) {
		this.tasks.put(taskId, task);
	}
	
	public boolean taskCompleted(int taskId) {
		if (tasks.get(taskId) != null) {
			return true;
		}
		
		return false;
	}
	
	public void saveCatalog() {
		FileOutputStream fos = null;
		ObjectOutputStream oos = null;
		try {
			fos = new FileOutputStream("tasks.catalog");
			oos = new ObjectOutputStream(fos);
			
			oos.writeObject(this.tasks);
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
			if (oos != null) {
				try {
					oos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
