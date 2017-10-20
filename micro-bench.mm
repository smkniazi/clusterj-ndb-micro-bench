<map version="1.0.1">
<!-- To view this file, download free mind mapping software FreeMind from http://freemind.sourceforge.net -->
<node CREATED="1508490728288" ID="ID_1940650692" MODIFIED="1508491730651">
<richcontent TYPE="NODE"><html>
  <head>
    
  </head>
  <body>
    <p>
      micro bench
    </p>
  </body>
</html>
</richcontent>
<node CREATED="1508490746414" HGAP="127" ID="ID_419391890" MODIFIED="1508491391692" POSITION="right" TEXT="PK" VSHIFT="-59">
<font BOLD="true" NAME="SansSerif" SIZE="12"/>
<node CREATED="1508490953699" ID="ID_1779465285" MODIFIED="1508494979575" TEXT="Table (id, part, data) PK(id,part)"/>
<node CREATED="1508491214268" ID="ID_471458471" MODIFIED="1508491220743" TEXT="id and partition id are same"/>
</node>
<node CREATED="1508490757147" HGAP="83" ID="ID_553659488" MODIFIED="1508491375228" POSITION="right" TEXT="Batched" VSHIFT="-41">
<font BOLD="true" NAME="SansSerif" SIZE="12"/>
<node CREATED="1508491249508" ID="ID_1431670659" MODIFIED="1508491265423" TEXT="Table (id, part data) PK(id,part)"/>
<node CREATED="1508491273179" ID="ID_1997242886" MODIFIED="1508492421061" TEXT="ishould all the batch operation resolve locally on the NDB node ">
<font BOLD="true" NAME="SansSerif" SIZE="12"/>
<icon BUILTIN="idea"/>
<node CREATED="1508492375243" ID="ID_1018281874" MODIFIED="1508492377179" TEXT="yes">
<node CREATED="1508492377180" ID="ID_581413054" MODIFIED="1508492394375" TEXT="part id = thread id"/>
</node>
<node CREATED="1508492396539" ID="ID_501312595" MODIFIED="1508492397619" TEXT="no ">
<node CREATED="1508492397620" ID="ID_208349886" MODIFIED="1508492402591" TEXT="partid = id"/>
</node>
</node>
<node CREATED="1508491287467" ID="ID_321778752" MODIFIED="1508491292767" TEXT="batch of size n"/>
</node>
<node CREATED="1508490760491" ID="ID_897432863" MODIFIED="1508491055908" POSITION="right" TEXT="PPIS">
<font BOLD="true" NAME="SansSerif" SIZE="12"/>
<node CREATED="1508491780843" ID="ID_1381285851" MODIFIED="1508491794135" TEXT="Table (id, part, data) PK id,part"/>
<node CREATED="1508494045572" ID="ID_1913526770" MODIFIED="1508494055984" TEXT="partiid = threadid"/>
<node CREATED="1508494634492" ID="ID_1382943882" MODIFIED="1508494636976" TEXT="size n"/>
</node>
<node CREATED="1508490766363" HGAP="42" ID="ID_1821738939" MODIFIED="1508494689925" POSITION="right" TEXT="IS" VSHIFT="59">
<font BOLD="true" NAME="SansSerif" SIZE="12"/>
<node CREATED="1508495022469" ID="ID_1773562165" MODIFIED="1508495240965">
<richcontent TYPE="NODE"><html>
  <head>
    
  </head>
  <body>
    <p>
      Table (id, pa rt, data)<b><font color="#cc0033">PK ID</font></b>
    </p>
  </body>
</html>
</richcontent>
<font NAME="SansSerif" SIZE="12"/>
<node CREATED="1508495134268" ID="ID_1125190294" MODIFIED="1508495271882" TEXT="index on part col">
<font BOLD="true" NAME="SansSerif" SIZE="12"/>
<icon BUILTIN="wizard"/>
</node>
</node>
<node CREATED="1508495078660" ID="ID_1931920368" MODIFIED="1508495819648" TEXT="part column is not part of pk"/>
<node CREATED="1508495820749" ID="ID_1985285389" MODIFIED="1508495832000" TEXT="should be distributed index scan so">
<node COLOR="#ff0000" CREATED="1508495832460" ID="ID_1406772790" MODIFIED="1508495863993" TEXT="partid = id">
<font BOLD="true" NAME="SansSerif" SIZE="12"/>
<icon BUILTIN="wizard"/>
</node>
</node>
<node CREATED="1508495104324" ID="ID_1632875995" MODIFIED="1508495105960" TEXT="size N"/>
</node>
<node CREATED="1508490769427" HGAP="44" ID="ID_827143135" MODIFIED="1508494862017" POSITION="right" TEXT="FTS" VSHIFT="91">
<font BOLD="true" NAME="SansSerif" SIZE="12"/>
<node CREATED="1508495605428" ID="ID_243484320" MODIFIED="1508495637136" TEXT="table (id, part, data) PK ID. No index on part key"/>
<node CREATED="1508495637836" ID="ID_295933044" MODIFIED="1508495890696" TEXT="part = id"/>
<node CREATED="1508495657004" ID="ID_1821348420" MODIFIED="1508495659176" TEXT="size n"/>
</node>
<node CREATED="1508491666907" HGAP="49" ID="ID_63086266" MODIFIED="1508495953415" POSITION="left" TEXT="Global Configuration" VSHIFT="-134">
<font BOLD="true" NAME="SansSerif" SIZE="12"/>
<icon BUILTIN="flag-green"/>
<node CREATED="1508491678499" ID="ID_302196011" MODIFIED="1508494304848" TEXT="Client ID. Domain [0,N]"/>
<node CREATED="1508491683315" ID="ID_567869239" MODIFIED="1508491697215" TEXT="Table ID start = client ID * 1 M"/>
<node CREATED="1508492148019" ID="ID_1156190524" MODIFIED="1508494243560" TEXT="thread ID Start = (client ID * numThreads)"/>
<node COLOR="#ff0000" CREATED="1508497059813" HGAP="14" ID="ID_473057784" MODIFIED="1508497309855" TEXT="Table ID Generator" VSHIFT="-11">
<font BOLD="true" NAME="SansSerif" SIZE="12"/>
<node CREATED="1508497068325" HGAP="17" ID="ID_72019240" MODIFIED="1508497225305" STYLE="fork" TEXT="row ID = (Global.TableID_START + (num Rows * Thread ID)" VSHIFT="-1"/>
</node>
</node>
<node CREATED="1508495350165" ID="ID_575422119" MODIFIED="1508495455543" POSITION="left" STYLE="bubble" TEXT="TABLES">
<node COLOR="#ff0000" CREATED="1508495356132" ID="ID_564434819" MODIFIED="1508495455543" STYLE="fork" TEXT="in all tables that have partition key part of the pk, put part col as first column"/>
</node>
</node>
</map>
