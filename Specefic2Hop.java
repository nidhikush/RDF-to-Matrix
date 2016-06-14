import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class Specefic2Hop {

	public static final String midresultspath = "E:\\datasets\\DBpedia2Movielens1Mv1.1\\midresult_direct.txt";
	public static final String finalresultspath = "E:\\datasets\\DBpedia2Movielens1Mv1.1\\finalresult.csv";

	public static void main(String args[]) throws Exception {
		String service = "http://dbpedia.org/sparql";
		// String service = "http://localhost:8890/sparql";
		System.setProperty("http.proxyPort", "8080");
		System.setProperty("http.proxyHost", "IP.IP.IP.IP");
		Authenticator.setDefault(new Authenticator() {
			@Override
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication("1234", "1234".toCharArray());
			}
		});
	        FileInputStream fstream = new FileInputStream(
				"E:\\datasets\\DBpedia2Movielens1Mv1.1\\Updated_Movielens2Lookup.csv"); //mapped file of DBpedia and MovieLens
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String db;

		BufferedWriter midwriter = new BufferedWriter(new FileWriter(
				midresultspath));

		//int limit = 3;

		while ((db = br.readLine()) != null) {

		//	if (limit == 0)
			//	break;
			//limit--;

		//	System.err.println(limit);
			System.out.println(db);
			String sparqlQueryString1 = "PREFIX dcterms: <http://purl.org/dc/terms/>"
					+ "\n"
					+ "select  ?dcterm_skos where {<"
					+ db
					+ "> dcterms:subject ?dcterms. ?dcterms skos:broader ?dcterm_skos}"; 
		//	 System.out.println(sparqlQueryString1);
			QueryExecution qexec = QueryExecutionFactory.sparqlService(service,
					sparqlQueryString1);

			midwriter.write(db);

			try {
				ResultSet forcnt = qexec.execSelect();

				int A = 0;
				for (; forcnt.hasNext();) {
					A++;
					forcnt.nextSolution();
				}

				ResultSet results = qexec.execSelect();
				for (; results.hasNext();) {
					QuerySolution soln = results.nextSolution();
					String y = soln.get("dcterm_skos").toString();
					// System.out.print(y + "\n");
					// bw.write(y +"\n");

					String details = "prefix dcterms: <http://purl.org/dc/terms/>select ?OtherMovies where { ?OtherMovies ?dcterms:subject ?subjectOther. ?subjectOther skos:broader <"
							+ y + "> } ";

					// System.out.println(details);

					QueryExecution detailexec = QueryExecutionFactory
							.sparqlService(service, details);
					ResultSet details_res = detailexec.execSelect();
					int B = 0;
					for (; details_res.hasNext();) {
						B++;
						details_res.nextSolution();
					}

					double result = calculateDev(A, B, 90063);
					// System.out.println("Total=" + result);
					midwriter.write(" ");
					midwriter.write(" ");
					midwriter.write(y);
					midwriter.write("#");
					midwriter.write(result + " ");
					midwriter.write("    ");
				}
				midwriter.write("\n");

			} finally {
				qexec.close();
			}

		}

		midwriter.close();
		Set<String> cols = getUniqueCol();
		processdata(cols);

	}

	private static void processdata(Set<String> cols) throws Exception {
		BufferedWriter finalwriter = null;
		try {
			finalwriter = new BufferedWriter(new FileWriter(finalresultspath));
			Iterator<String> itr = cols.iterator();
			finalwriter.write("ros");
			finalwriter.write("\t");
			int count = 0;
			while (itr.hasNext()) {
				finalwriter.write(itr.next());
				finalwriter.write("\t");
				count++;
			}
			System.out.println("count=" + count);
			finalwriter.write("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		BufferedReader br = new BufferedReader(new FileReader(midresultspath));
		HashMap<String, String> map = new HashMap<String, String>();
		String linedata = null;
		while ((linedata = br.readLine()) != null) {
			String linedataSplit[] = linedata.split("\\s+");
			finalwriter.write(linedataSplit[0]);
			finalwriter.write("\t");
			for (int i = 1; i < linedataSplit.length; i++) {
				String dburl[] = linedataSplit[i].split("#");
				// System.out.println("for split=" + linedataSplit[i]);
				map.put(dburl[0], dburl[1]);
			}
			Iterator<String> itr = cols.iterator();
			while (itr.hasNext()) {
				String key = itr.next();
				if (map.containsKey(key)) {
					finalwriter.write(map.get(key));
				} else {
					finalwriter.write("0");
				}
				finalwriter.write("\t");
			}
			finalwriter.write("\n");
			map.clear();
		}

		finalwriter.close();
	}

	static Set<String> getUniqueCol() {

		Set<String> set = new LinkedHashSet<String>();
		List<String> list = new LinkedList<String>();

		try {
			BufferedReader br = new BufferedReader(new FileReader(
					midresultspath));
			String line = null;
			while ((line = br.readLine()) != null) {
				String linedata[] = line.split("\\s+");
				for (int i = 1; i < linedata.length; i++) {
					String catdetails[] = linedata[i].split("#");
					set.add(catdetails[0]);
					list.add(catdetails[0]);
				}
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// System.out.println(set);
		// System.out.println(list);
		// System.out.println(set.size());
		// System.out.println(list.size());
		return set;
	}

	static double calculateDev(int a, int b, int N) {
		// System.out.println(a);
		double result = (1.0 / a) * (Math.log(N) / Math.log(b));
		return result;
	}
}


