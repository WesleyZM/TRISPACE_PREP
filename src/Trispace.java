import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import processing.core.PApplet;
//import processing.core.*;
import processing.data.Table;

public class Trispace {
	PApplet parent;
	PrintWriter output;
	Table inputTable;
	Table initialTable;
	int locusCount;
	int timeCount;
	int attCount;
	public String destFile;
	String inputType;
	String formatOfData;
	Table attDictionary;
	Table timeDictionary;
	Table locusDictionary;
	public Table A_LT = null;
	public Table T_LA = null;
	public Table AT_L = null;
	public Table LA_T = null;
	public Table LT_A = null;
	public Table L_AT = null;
	public Table A_LTsin = null;
	public Table T_LAsin = null;
	public Table AT_Lsin = null;
	public Table LA_Tsin = null;
	public Table LT_Asin = null;
	public Table L_ATsin = null;
	String dataIn_L_AT = "L_AT";
	String dataIn_A_LT = "A_LT";
	String dataIn_A_TL = "A_TL";
	String dataIn_T_LA = "T_LA";
	String dataIn_T_AL = "T_AL";
	public ArrayList<Table> perspectivesDoub = new ArrayList();
	public ArrayList<Table> perspectivesSin = new ArrayList();
	public ArrayList<String> perspectivesSOMatic = new ArrayList();
	boolean saveSing;
	boolean saveDoub;
	boolean somData;

	public Trispace(PApplet p, String type, String t, int locusCount_, int timeCount_, int attCount_, String destination, boolean saveSing_, boolean saveDoub_, boolean somData_)
	{
		this.parent = p;
		this.inputTable = parent.loadTable(t);
		this.initialTable = parent.loadTable(t);
		this.inputType = type;
		this.formatOfData = type;
		this.locusCount = locusCount_;
		this.timeCount = timeCount_;
		this.attCount = attCount_;
		this.destFile = destination;
		this.somData = somData_;
		this.attDictionary = new Table();
		this.timeDictionary = new Table();
		this.locusDictionary = new Table();
		this.saveSing = saveSing_;
		this.saveDoub = saveDoub_;
	}

	public void processInput()
	{
		createAllDictionaries(this.inputTable);
		if (this.inputType.equals("L_AT"))
		{
			System.out.println("Input L_AT");
			this.L_AT = this.inputTable;

			this.initialTable.setString(0, 0, "L_AT");
			this.perspectivesDoub.add(this.initialTable);
			this.L_ATsin = this.initialTable;
			convertToAll(this.inputTable);
		}
		else if (this.inputType.equals("A_LT"))
		{
			this.A_LT = this.inputTable;

			convertToAll(fromAT_LtoL_AT(this.inputTable));
		}
		else if (this.inputType.equals("T_LA"))
		{
			this.T_LA = this.inputTable;

			convertToAll(fromT_LAtoL_AT(this.inputTable));
		}
		else if (this.inputType.equals("AT_L"))
		{
			this.AT_L = this.inputTable;

			convertToAll(fromAT_LtoL_AT(this.inputTable));
		}
		else if (this.inputType.equals("LT_A"))
		{
			this.LT_A = this.inputTable;

			convertToAll(fromLT_AtoL_AT(this.inputTable));
		}
		else if (this.inputType.equals("LA_T"))
		{
			this.LA_T = this.inputTable;

			convertToAll(fromLA_TtoL_AT(this.inputTable));
		}
	}

	void convertToAll(Table l_at)
	{
		if (this.L_AT == null)
		{
			this.L_AT = l_at;
			this.L_AT.setString(0, 0, "L_AT");
			this.perspectivesDoub.add(this.L_AT);
			this.L_ATsin = this.initialTable;
		}
		if (this.AT_L == null)
		{
			this.AT_L = convertFromL_ATtoAT_L();
			this.AT_L.setString(0, 0, "AT_L");
			this.perspectivesDoub.add(this.AT_L);
			this.AT_Lsin = this.AT_L;
		}
		if (this.A_LT == null)
		{
			this.A_LT = l_atTOa_ltTest(l_at);
			this.A_LT.setString(0, 0, "A_LT");
			this.perspectivesDoub.add(this.A_LT);
			this.A_LTsin = this.A_LT;
		}
		if (this.T_LA == null)
		{
			this.T_LA = convertFromL_ATtoT_LA(l_at);
			this.T_LA.setString(0, 0, "T_LA");
			this.perspectivesDoub.add(this.T_LA);
			this.T_LAsin = this.T_LA;
		}
		if (this.LT_A == null)
		{
			this.LT_A = convertFromL_ATtoLT_A();
			this.LT_A.setString(0, 0, "LT_A");
			this.perspectivesDoub.add(this.LT_A);
			this.LT_Asin = this.LT_A;
		}
		if (this.LA_T == null)
		{
			this.LA_T = convertFromL_ATtoLA_T();
			this.LA_T.setString(0, 0, "LA_T");
			this.perspectivesDoub.add(this.LA_T);
			this.LA_Tsin = this.LA_T;
		}
		if (this.saveDoub) {
			for (int i = 0; i < this.perspectivesDoub.size(); i++) {
				parent.saveTable((Table)this.perspectivesDoub.get(i), this.destFile + 
						"\\transformations\\" + 
						((Table)this.perspectivesDoub.get(i)).getString(0, 0) + ".csv");
			}
		}
		joinLabels(this.L_ATsin, "L_AT", "col");
		joinLabels(this.A_LTsin, "A_TL", "col");
		joinLabels(this.T_LAsin, "T_LA", "col");
		joinLabels(this.AT_Lsin, "AT_L", "row");
		joinLabels(this.LT_Asin, "LT_A", "row");
		joinLabels(this.LA_Tsin, "LA_T", "row");
		if (this.saveSing) {
			for (int i = 0; i < this.perspectivesSin.size(); i++) {
				parent.saveTable((Table)this.perspectivesSin.get(i), this.destFile + 
						"\\SOM\\" + 
						((Table)this.perspectivesSin.get(i)).getString(0, 0)
						.substring(0, 4) + ".csv");
			}
		}
	}

	public Table l_atTOa_ltTest(Table startTable)
	{
		this.formatOfData = this.dataIn_L_AT;

		startTable.removeColumn(0);
		startTable.removeRow(1);
		startTable.removeRow(0);
		Table endA_LT = new Table();
		int row = 0;
		int col = 0;
		int currTime = 0;
		String value = "";
		for (int i = 0; i < startTable.getRowCount(); i++) {
			for (int j = 0; j < startTable.getColumnCount(); j++)
			{
				row = (int)Math.floor(j / this.timeCount);
				col = currTime + i * this.timeCount;
				value = startTable.getString(i, j);
				endA_LT.setString(row + 2, col + 1, value);
				if (currTime < this.timeCount - 1) {
					currTime++;
				} else {
					currTime = 0;
				}
			}
		}
		setLabelsA_LT(endA_LT);
		return endA_LT;
	}

	public Table convertFromL_ATtoT_LA(Table startTable)
	{
		this.formatOfData = this.dataIn_L_AT;

		Table endT_LA = new Table();

		int row = 0;
		int col = 0;
		String value = "";
		for (int i = 0; i < startTable.getRowCount(); i++) {
			for (int j = 0; j < startTable.getColumnCount(); j++)
			{
				value = startTable.getString(i, j);
				row = j % this.timeCount;

				col = (int)(i * this.attCount + Math.floor(j / 
						this.timeCount));

				endT_LA.setString(row + 2, col + 1, value);
			}
		}
		setLabelsT_LA(endT_LA);
		return endT_LA;
	}

	public Table convertFromL_ATtoAT_L()
	{
		Table AT_Lnew = new Table();
		for (int i = 0; i < this.L_AT.getColumnCount(); i++) {
			for (int j = 0; j < this.L_AT.getRowCount(); j++) {
				AT_Lnew.setString(i, j, this.L_AT.getString(j, i));
			}
		}
		this.AT_L = AT_Lnew;
		return AT_Lnew;
	}

	public Table convertFromL_ATtoLA_T()
	{
		Table LA_Tnew = new Table();
		for (int i = 0; i < this.T_LA.getColumnCount(); i++) {
			for (int j = 0; j < this.T_LA.getRowCount(); j++) {
				LA_Tnew.setString(i, j, this.T_LA.getString(j, i));
			}
		}
		this.LA_T = LA_Tnew;
		return LA_Tnew;
	}

	public Table convertFromL_ATtoLT_A()
	{
		Table LT_Anew = new Table();
		for (int i = 0; i < this.A_LT.getColumnCount(); i++) {
			for (int j = 0; j < this.A_LT.getRowCount(); j++) {
				LT_Anew.setString(i, j, this.A_LT.getString(j, i));
			}
		}
		this.LT_A = LT_Anew;
		return LT_Anew;
	}

	public Table fromAT_LtoL_AT(Table t)
	{
		Table LA_Tnew = new Table();
		for (int i = 0; i < t.getColumnCount(); i++) {
			for (int j = 0; j < t.getRowCount(); j++) {
				LA_Tnew.setString(i, j, t.getString(j, i));
			}
		}
		return LA_Tnew;
	}

	public Table fromA_LTtoL_AT(Table startTable)
	{
		this.formatOfData = this.dataIn_A_LT;

		startTable.removeColumn(0);
		startTable.removeRow(1);
		startTable.removeRow(0);
		Table endL_AT = new Table();
		int row = 0;
		int col = 0;
		int currTime = 0;
		String value = "";
		for (int i = 0; i < startTable.getRowCount(); i++) {
			for (int j = 0; j < startTable.getColumnCount(); j++)
			{
				row = (int)Math.floor(j / this.timeCount);
				col = currTime + i * this.timeCount;
				value = startTable.getString(i, j);
				endL_AT.setString(row + 2, col + 1, value);
				if (currTime < this.timeCount - 1) {
					currTime++;
				} else {
					currTime = 0;
				}
			}
		}
		setLabelsL_AT(endL_AT);
		return endL_AT;
	}

	public Table fromLT_AtoL_AT(Table startLT_A)
	{
		Table temp = new Table();
		for (int i = 0; i < startLT_A.getColumnCount(); i++) {
			for (int j = 0; j < startLT_A.getRowCount(); j++) {
				temp.setString(i, j, startLT_A.getString(j, i));
			}
		}
		return fromA_LTtoL_AT(temp);
	}

	public Table fromT_LAtoL_AT(Table startTable)
	{
		this.formatOfData = this.dataIn_T_LA;

		startTable.removeColumn(0);
		startTable.removeRow(1);
		startTable.removeRow(0);
		Table endL_AT = new Table();
		int currAtt = 0;
		int row = 0;
		int col = 0;
		String value = "";
		for (int i = 0; i < startTable.getRowCount(); i++) {
			for (int j = 0; j < startTable.getColumnCount(); j++)
			{
				value = startTable.getString(i, j);
				row = (int)Math.floor(j / this.attCount);
				col = currAtt * this.timeCount + i;
				endL_AT.setString(row + 2, col + 1, value);
				if (currAtt < this.attCount - 1) {
					currAtt++;
				} else {
					currAtt = 0;
				}
			}
		}
		setLabelsL_AT(endL_AT);
		return endL_AT;
	}

	public Table fromLA_TtoL_AT(Table startLA_T)
	{
		Table temp = new Table();
		for (int i = 0; i < startLA_T.getColumnCount(); i++) {
			for (int j = 0; j < startLA_T.getRowCount(); j++) {
				temp.setString(i, j, startLA_T.getString(j, i));
			}
		}
		return fromT_LAtoL_AT(temp);
	}

	public static Trispace createTrispace(PApplet p, String type, String t, int locusCount_, int timeCount_, int attCount_, String destination, boolean wantSingHeader, boolean wantDoubHeader, boolean wantSOMDat)
	{
		Trispace ts = new Trispace(p,type, t, locusCount_, timeCount_, attCount_, destination, wantSingHeader, wantDoubHeader, wantSOMDat);
		System.out.println(ts.inputTable.getColumnCount());
		ts.processInput();
		if (ts.somData) {
			for (int i = 0; i < ts.perspectivesSin.size(); i++)
			{
				ts.writeFile((Table)ts.perspectivesSin.get(i), 
						ts.destFile + "//SOMaticIn//" + ((Table)ts.perspectivesSin.get(i)).getString(0, 0).substring(0, 4) + ".dat");
				ts.perspectivesSOMatic.add(ts.destFile + "//SOMaticIn//" + ((Table)ts.perspectivesSin.get(i)).getString(0, 0).substring(0, 4) + 
						".dat");
			}
		}
		return ts;
	}

	private void writeFile(Table t, String file)
	{
		this.output = parent.createWriter(file);
		this.output.println(t.getColumnCount() - 1);
		String n = "";

		n = n + "#att ";
		for (int i = 1; i < t.getColumnCount(); i++)
		{
			n = n + t.getString(0, i);
			if (i < t.getColumnCount()) {
				n = n + " ";
			}
		}
		this.output.println(n);
		n = "";
		for (int i = 1; i < t.getRowCount(); i++)
		{
			for (int j = 1; j < t.getColumnCount(); j++) {
				n = n + t.getString(i, j) + " ";
			}
			n = n + t.getString(i, 0) + " " + i;
			this.output.println(n);

			n = "";
		}
		this.output.flush();
		this.output.close();
	}

	public void createAttDictionary(Table t)
	{
		for (int i = 0; i < attCount; i++) {
		    attDictionary.setString(i,0,"a"+(i+1));
		    if(formatOfData==dataIn_L_AT){
		      attDictionary.setString(i,1,t.getString(0,((i*timeCount)+1)));
		    } else if(formatOfData==dataIn_A_LT || formatOfData==dataIn_A_TL){
		      attDictionary.setString(i,1,t.getString(i+2,0));
		    } else if(formatOfData==dataIn_T_AL) {
		      attDictionary.setString(i,1,t.getString(0,((i*locusCount)+1)));
		    } else if (formatOfData== dataIn_T_LA) {
		      attDictionary.setString(i,1,t.getString(1, i+1));
		    }
		  }
		  parent.saveTable(attDictionary,"data\\dictionary\\attDictionary.csv");
//		for (int i = 0; i < this.attCount; i++)
//		{
//			this.attDictionary.setString(i, 0, "a" + (i + 1));
//			if (this.formatOfData == this.dataIn_L_AT) {
//				this.attDictionary.setString(i, 1, 
//						t.getString(0, i * this.timeCount + 1));
//			} else if ((this.formatOfData == this.dataIn_A_LT) || 
//					(this.formatOfData == this.dataIn_A_TL)) {
//				this.attDictionary.setString(i, 1, t.getString(i + 2, 0));
//			} else if (this.formatOfData == this.dataIn_T_AL) {
//				this.attDictionary.setString(i, 1, 
//						t.getString(0, i * this.locusCount + 1));
//			} else if (this.formatOfData == this.dataIn_T_LA) {
//				this.attDictionary.setString(i, 1, t.getString(1, i + 1));
//			}
//		}
//		parent.saveTable(this.attDictionary, this.destFile + "dictionary\\attDictionary.csv");
	}

	public void createTimeDictionary(Table t)
	{
		for (int i = 0; i < timeCount; i++) {
		    timeDictionary.setString(i,0,"t"+(i+1));
		    
		    if(formatOfData==dataIn_L_AT){
		      timeDictionary.setString(i,1,t.getString(1,(i+1)));
		    } else if(formatOfData==dataIn_T_LA || formatOfData==dataIn_T_AL){
		      timeDictionary.setString(i,1,t.getString(i+2,0));
		    } else if(formatOfData==dataIn_A_TL) {
		       timeDictionary.setString(i,1,t.getString(0, (i*locusCount)+1));
		    } else if (formatOfData== dataIn_A_LT) {
		       timeDictionary.setString(i,1,t.getString(1,i+1));
		    }
		  }
		  parent.saveTable(timeDictionary,"data\\dictionary\\timeDictionary.csv");
//		for (int i = 0; i < this.timeCount; i++)
//		{
//			this.timeDictionary.setString(i, 0, "t" + (i + 1));
//			if (this.formatOfData == this.dataIn_L_AT) {
//				this.timeDictionary.setString(i, 1, t.getString(1, i + 1));
//			} else if ((this.formatOfData == this.dataIn_T_LA) || 
//					(this.formatOfData == this.dataIn_T_AL)) {
//				this.timeDictionary.setString(i, 1, t.getString(i + 2, 0));
//			} else if (this.formatOfData == this.dataIn_A_TL) {
//				this.timeDictionary.setString(i, 1, 
//						t.getString(0, i * this.locusCount + 1));
//			} else if (this.formatOfData == this.dataIn_A_LT) {
//				this.timeDictionary.setString(i, 1, t.getString(1, i + 1));
//			}
//		}
//		parent.saveTable(this.timeDictionary, this.destFile + "dictionary\\timeDictionary.csv");
	}

	public void creatLocusDictionary(Table t)
	{
//		parent.println("locus dictionary");
		for (int i = 0; i < locusCount; i++) {
			locusDictionary.setString(i,0,"l"+(i+1));

			if(this.formatOfData==dataIn_L_AT){
//				parent.println("data in L_AT");
				locusDictionary.setString(i,1,t.getString(i+2,0));
				//		      println(t.getString(i+2,0));
//				    parent.println("data in LAT");
			} else if(this.formatOfData==dataIn_T_LA){
//				parent.println("data in T_LA");
				locusDictionary.setString(i,1,t.getString(0, (i*attCount)+1));
			} else if(this.formatOfData==dataIn_T_AL){
//				parent.println("data in T_AL");
				locusDictionary.setString(i,1,t.getString(1,i+1));
			} else if(this.formatOfData==dataIn_A_TL) {
				locusDictionary.setString(i,1,t.getString(1,i+1));
			} else if (this.formatOfData== dataIn_A_LT) {
//						      parent.println("currLocus: " + t.getString(0,(i*timeCount)+1));
				locusDictionary.setString(i,1,t.getString(0,(i*timeCount)+1));
			}

		}
		parent.saveTable(locusDictionary,"data\\dictionary\\locusDictionary.csv");
//		for (int i = 0; i < this.locusCount; i++)
//		{
//			this.locusDictionary.setString(i, 0, "l" + (i + 1));
//			if (this.formatOfData == this.dataIn_L_AT) {
//				this.locusDictionary.setString(i, 1, t.getString(i + 2, 0));
//			} else if (this.formatOfData == this.dataIn_T_LA) {
//				this.locusDictionary.setString(i, 1, 
//						t.getString(0, i * this.attCount + 1));
//			} else if (this.formatOfData == this.dataIn_T_AL) {
//				this.locusDictionary.setString(i, 1, t.getString(1, i + 1));
//			} else if (this.formatOfData == this.dataIn_A_TL) {
//				this.locusDictionary.setString(i, 1, t.getString(1, i + 1));
//			} else if (this.formatOfData == this.dataIn_A_LT) {
//				this.locusDictionary.setString(i, 1, 
//						t.getString(0, i * this.timeCount + 1));
//			}
//		}
//		parent.saveTable(this.locusDictionary, this.destFile + "dictionary\\locusDictionary.csv");
	}

	public void createAllDictionaries(Table tableForDictionaries)
	{
		creatLocusDictionary(tableForDictionaries);
		createAttDictionary(tableForDictionaries);
		createTimeDictionary(tableForDictionaries);
	}

	public String setAttributeLabel(String att)
	{
		for (int l = 0; l < this.attDictionary.getRowCount(); l++) {
			if (att.equals(this.attDictionary.getString(l, 0)))
			{
				att = this.attDictionary.getString(l, 1);
				break;
			}
		}
		return att;
	}

	public String setTimeLabel(String time)
	{
		for (int l = 0; l < this.timeDictionary.getRowCount(); l++) {
			if (time.equals(this.timeDictionary.getString(l, 0)))
			{
				time = this.timeDictionary.getString(l, 1);
				break;
			}
		}
		return time;
	}

	public String setLocusLabel(String locus)
	{
		for (int l = 0; l < this.locusDictionary.getRowCount(); l++) {
			if (locus.equals(this.locusDictionary.getString(l, 0)))
			{
				locus = this.locusDictionary.getString(l, 1);
				break;
			}
		}
		return locus;
	}

	public void joinLabels(Table t, String name, String joinRowOrCol)
	{
		String lab = "";
		if (joinRowOrCol == "row")
		{
			for (int i = 0; i < t.getRowCount(); i++)
			{
				lab = t.getString(i, 0) + "_" + t.getString(i, 1);
				t.setString(i, 0, lab);
			}
			t.removeColumn(1);
		}
		else
		{
			for (int i = 0; i < t.getColumnCount(); i++)
			{
				lab = t.getString(0, i) + "_" + t.getString(1, i);
				t.setString(0, i, lab);
			}
			t.removeRow(1);
		}
		this.perspectivesSin.add(t);
	}

	public void assignTableHeaderSingleObject(Table t, String tableType)
	{
		String row1 = "";
		String row2 = "";
		int currLocus = 1;
		int currTime = 1;
		int currCol = 1;
		if (tableType.equals("a_lt")) {
			for (int i = 0; i < this.locusCount; i++)
			{
				for (int j = 0; j < this.timeCount; j++)
				{
					row1 = "l";
					row2 = "t";
					row1 = row1 + currLocus;
					row2 = row2 + currTime;
					t.setString(0, currCol, setLocusLabel(row1));
					t.setString(1, currCol, setTimeLabel(row2));

					currCol++;
					currTime++;
					if (currTime > this.timeCount) {
						currTime = 1;
					}
				}
				currLocus++;
				if (currLocus > this.locusCount) {
					currLocus = 1;
				}
			}
		} else if (tableType.equals("t_al")) {
			for (int i = 0; i < this.locusCount; i++)
			{
				for (int j = 0; j < this.attCount; j++)
				{
					row1 = "l";
					row2 = "a";
					row1 = row1 + currLocus;
					row2 = row2 + currTime;
					t.setString(0, currCol, setLocusLabel(row1));
					t.setString(1, currCol, setAttributeLabel(row2));
					currCol++;
					currTime++;
					if (currTime > this.attCount) {
						currTime = 1;
					}
				}
				currLocus++;
				if (currLocus > this.locusCount) {
					currLocus = 1;
				}
			}
		}
	}

	public void assignLabelsForL_AT(Table t)
	{
		for (int i = 0; i < this.attCount * this.timeCount; i++) {
			t.setString(0, i + 1, "a" + (
					(int)Math.floor(i / this.attCount) + 1));
		}
		int currTime = 1;
		for (int i = 0; i < this.attCount * this.timeCount; i++)
		{
			t.setString(1, i + 1, "t" + currTime);
			if (currTime < this.timeCount) {
				currTime++;
			} else {
				currTime = 1;
			}
		}
		for (int i = 0; i < this.locusCount; i++) {
			t.setString(i + 2, 0, "l" + (i + 1));
		}
	}

	void setLabelsL_AT(Table table)
	{
		for (int i = 0; i < this.locusCount; i++) {
			table.setString(i + 2, 0, this.locusDictionary.getString(i, 1));
		}
		int currTime = 0;
		for (int i = 0; i < this.timeCount * this.attCount; i++)
		{
			table.setString(
					0, 
					i + 1, 
					this.attDictionary.getString(
							(int)Math.floor(i / this.timeCount), 1));
			table.setString(1, i + 1, this.timeDictionary.getString(currTime, 1));
			if (currTime < this.timeCount - 1) {
				currTime++;
			} else {
				currTime = 0;
			}
		}
	}

	void setLabelsA_LT(Table table)
	{
		for (int i = 0; i < this.attCount; i++) {
			table.setString(i + 2, 0, this.attDictionary.getString(i, 1));
		}
		int currTime = 0;
		for (int i = 0; i < this.timeCount * this.locusCount; i++)
		{
			table.setString(
					0, 
					i + 1, 
					this.locusDictionary.getString(
							(int)Math.floor(i / this.timeCount), 1));
			table.setString(1, i + 1, this.timeDictionary.getString(currTime, 1));
			if (currTime < this.timeCount - 1) {
				currTime++;
			} else {
				currTime = 0;
			}
		}
	}

	void setLabelsT_LA(Table table)
	{
		for (int i = 0; i < this.timeCount; i++) {
			table.setString(i + 2, 0, this.timeDictionary.getString(i, 1));
		}
		int curratt = 0;
		for (int i = 0; i < this.attCount * this.locusCount; i++)
		{
			table.setString(
					0, 
					i + 1, 
					this.locusDictionary.getString(
							(int)Math.floor(i / this.attCount), 1));
			table.setString(1, i + 1, this.attDictionary.getString(curratt, 1));
			if (curratt < this.attCount - 1) {
				curratt++;
			} else {
				curratt = 0;
			}
		}
	}
}
