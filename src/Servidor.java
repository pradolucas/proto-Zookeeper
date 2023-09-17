import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import com.google.gson.Gson;

public class Servidor {
	private InetAddress ip;
	private int port;
	public boolean leader;
	private InetAddress ipLead;
	private int portLead;
	private long timestamp;

	// Estruturas de dados para armazenar o hash dos valores e timestamps
	private static Map<String, String> hashVal;
	private static Map<String, Long> hashTime;
	public static List<SimpleEntry<InetAddress, Integer>> serversSub = new ArrayList<>(); // Lista de servidores
																							// subordinados ao líder

	public Servidor(InetAddress ip, int port, InetAddress ipLead, int portLead) throws IOException {
		this.setIp(ip);
		this.setPort(port);
		this.ipLead = ipLead;
		this.portLead = portLead;
		if (ip.equals(ipLead) && port == portLead) // Verificação se este servidor é o líder

			this.leader = true;
		else {
			this.leader = false;
			this.joinLeader();
		}
		hashVal = new HashMap<>();
		hashTime = new HashMap<>();
		timestamp = (new Timestamp(System.currentTimeMillis())).getTime(); // Tomar o tempo de instanciação

	}

	// Método para que um server se junte ao líder
	private void joinLeader() throws IOException {
		Socket notifyLeadSoc = new Socket(this.ipLead, this.portLead);
		OutputStream Ostream = notifyLeadSoc.getOutputStream();
		DataOutputStream Writer = new DataOutputStream(Ostream);
		Mensagem nLOMsg = new Mensagem("NULL", "NULL", "JOIN", 0, getIp(), getPort(), this.ipLead, this.portLead);
		String nLOMsgJson = new Gson().toJson(nLOMsg);
		Writer.writeBytes(nLOMsgJson + "\n");
		notifyLeadSoc.close();
	}

	public InetAddress getIp() {
		return ip;
	}

	public void setIp(InetAddress ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	// Método para calcular o timestamp considerando 0 como o momento de
	// instanciação
	private long getTimestamp() {
		return (new Timestamp(System.currentTimeMillis())).getTime() - this.timestamp;
	}

	// Função que designa uma Thread para aceitar requisições de conexão do cliente
	// e de outros servers
	public void listen(int port) throws UnknownHostException, IOException {
		Servidor s = this;

		new Thread() {
			private ServerSocket listenSocket;

			public void run() {
				try {
					listenSocket = new ServerSocket(port);
					while (true) {
						Socket connectSocket = listenSocket.accept();
						ConnThread requestThread = new ConnThread(connectSocket, s);
						requestThread.start();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}.start();
	}

	// Classe aninhada representando uma thread para lidar com as conexões recebidas
	static class ConnThread extends Thread {
		private Socket soc;
		private Servidor s;

		public ConnThread(Socket soc, Servidor s) {
			this.soc = soc;
			this.s = s;
		}

		@Override
		public void run() {
			try {
				// Leitura de mensagem
				Mensagem msgMessage = getResponse();

				// Tratamento da mensagem recebida
				s.msgHandling(msgMessage, soc);

				// Closing connection
				soc.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private Mensagem getResponse() throws IOException {
			InputStreamReader istream = new InputStreamReader(soc.getInputStream());
			BufferedReader reader = new BufferedReader(istream);
			String msgRes = reader.readLine();
			Mensagem msgMessage = new Gson().fromJson(msgRes, Mensagem.class);
			return msgMessage;
		}
	}

	private void msgHandling(Mensagem msg, Socket soc) throws IOException {
		String msgType = msg.getType();
		if (msgType.equals("PUT")) {
			if (leader) {
				// Caso que o processamento está sendo feito pelo server
				System.out.println("Cliente " + msg.getIpS().getHostAddress() + ":" + msg.getPortS() + " PUT key:"
						+ msg.getKey() + " value:" + msg.getValue());

				// Atualizar a tabela local com o valor recebido
				long putTs = getTimestamp();
				hashVal.put(msg.getKey(), msg.getValue());
				hashTime.put(msg.getKey(), putTs);

				int allReplicationOk = 0;
				// REPLICATION para outros servers
				for (SimpleEntry<InetAddress, Integer> server : serversSub) {
					Socket sendNb = new Socket(server.getKey(), server.getValue());
					// Envio de mensagem para outro server
					OutputStream nbOstream = sendNb.getOutputStream();
					DataOutputStream nbWriter = new DataOutputStream(nbOstream);
					Mensagem nbMsg = new Mensagem(msg.getKey(), msg.getValue(), "REPLICATION", putTs, getIp(),
							getPort(), server.getKey(), server.getValue());
					String nbMsgJson = new Gson().toJson(nbMsg);
					nbWriter.writeBytes(nbMsgJson + "\n");

					InputStreamReader istream = new InputStreamReader(sendNb.getInputStream());
					BufferedReader reader = new BufferedReader(istream);

					// Leitura de mensagem
					String sendNbRes = reader.readLine();

					// Closing connection
					sendNb.close();

					// Casting da resposta recebida
					Mensagem msgMessage = new Gson().fromJson(sendNbRes, Mensagem.class);
					if (msgMessage.getType().equals("REPLICATION_OK")) { // Checa se servidores receberam REPLICATION
						allReplicationOk += 1;
					}
				}

				// Envio de PUT_OK para o cliente
				if (allReplicationOk == Servidor.serversSub.size()) {
					System.out.println("Enviando PUT_OK ao Cliente " + msg.getIpS().getHostAddress() + ":"
							+ msg.getPortS() + " da key:" + msg.getKey() + " ts:" + putTs);
					// Stream para Escrita
					Socket sendCli = new Socket(msg.getIpS(), msg.getPortS());
					OutputStream ostream = sendCli.getOutputStream();
					DataOutputStream writer = new DataOutputStream(ostream);

					Mensagem cliMsg = new Mensagem(msg.getKey(), msg.getValue(), "PUT_OK", putTs, getIp(), getPort(),
							msg.getIpS(), msg.getPortS());
					String cliMsgJson = new Gson().toJson(cliMsg);
					writer.writeBytes(cliMsgJson + "\n");
					// closing connection
					sendCli.close();
					writer.close();
				}
			} else {
				// Encaminhando PUT para o líder
				System.out.println("Encaminhando PUT key:" + msg.getKey() + " value:" + msg.getValue());

				// Socket para conexão
				Socket sendLead = new Socket(ipLead, portLead);
				String leadMsgJson = new Gson().toJson(msg);
				// Envio de mensagem
				OutputStream ostream = sendLead.getOutputStream();
				DataOutputStream writer = new DataOutputStream(ostream);
				writer.writeBytes(leadMsgJson + "\n");
				// Fechando conn
				sendLead.close();
				writer.close();
			}
		} else if (msgType.equals("GET")) {
			String val = hashVal.get(msg.getKey());
			Long valTs;
			if (hashTime.get(msg.getKey()) == null) // Caso chave não esteja presente na estrutura considere o timestamp
													// 0, neste caso, adotado como null
				valTs = (long) 0;
			else
				valTs = hashTime.get(msg.getKey());

			Socket cliSoc = new Socket(msg.getIpS(), msg.getPortS());
			OutputStream ostream = cliSoc.getOutputStream();
			DataOutputStream writer = new DataOutputStream(ostream);

			System.out.print("Cliente " + msg.getIpS().getHostAddress() + ":" + msg.getPortS() + " GET key:"
					+ msg.getKey() + " ts:" + msg.getTimestamp() + ". Meu ts é " + valTs + ", portanto devolvendo ");

			if (valTs < msg.getTimestamp()) {
				// Caso Chave desatualizadaa
				Mensagem resMsg = new Mensagem(msg.getKey(), "TRY_OTHER_SERVER_OR_LATER", "GET", valTs, getIp(),
						getPort(), msg.getIpS(), msg.getPortS());
				String msgJson = new Gson().toJson(resMsg);
				writer.writeBytes(msgJson + "\n");
				System.out.println("TRY_OTHER_SERVER_OR_LATER");
			} else if (val == null) {
				// Chave não presente na estrutura local
				Mensagem resMsg = new Mensagem(msg.getKey(), "NULL", "GET", valTs, getIp(), getPort(), msg.getIpS(),
						msg.getPortS());
				String msgJson = new Gson().toJson(resMsg);
				writer.writeBytes(msgJson + "\n");
				System.out.println("NULL");
			} else {
				// Caso chave atualizada
				Mensagem resMsg = new Mensagem(msg.getKey(), val, "GET", valTs, getIp(), getPort(), msg.getIpS(),
						msg.getPortS());
				String msgJson = new Gson().toJson(resMsg);
				writer.writeBytes(msgJson + "\n");
				System.out.println(val);
			}
			// Fechando conn
			cliSoc.close();
		} else if (msgType.equals("REPLICATION")) {
			System.out.println(
					"REPLICATION key:" + msg.getKey() + " value:" + msg.getValue() + " ts:" + msg.getTimestamp());
			Random random = new Random();
			boolean randomBoolean = random.nextBoolean();
			if (randomBoolean) { // Adicionando fator de aleatóridade a atualização das chaves nos servidores.
									// Para verificação do recurso TRY_OTHER_SERVER_OR_LATER
				// Atualizar a tabela local com o valor recebido
				hashVal.put(msg.getKey(), msg.getValue());
				hashTime.put(msg.getKey(), msg.getTimestamp());
			}
			// Envio de resposta para o líder
			OutputStream ostream = soc.getOutputStream();
			DataOutputStream writer = new DataOutputStream(ostream);
			Mensagem resMsg = new Mensagem(msg.getKey(), msg.getValue(), "REPLICATION_OK", getTimestamp(), getIp(),
					getPort(), msg.getIpS(), msg.getPortS());
			String resMsgJson = new Gson().toJson(resMsg);

			writer.writeBytes(resMsgJson + "\n");

			// Fechando conn
			soc.close();
		} else if (msgType.equals("JOIN")) {
			// Adicionando o servidor à lista de servidores subordinados
			serversSub.add(new SimpleEntry<InetAddress, Integer>(msg.getIpS(), msg.getPortS()));
		}
		System.out.println("\n");
	}

	public static void main(String[] args) throws UnknownHostException, IOException {
		Scanner scanner = new Scanner(System.in);
		System.out.print("Insira o IP do servidor\n>>>");
		InetAddress ip = InetAddress.getByName(scanner.next());
		System.out.print("Insira a porta do servidor\n>>>");
		int port = scanner.nextInt();

		System.out.print("Insira o IP do servidor líder\n>>>");
		InetAddress ipLeader = InetAddress.getByName(scanner.next());
		System.out.print("Insira a porta do servidor líder\n>>>");
		int portLeader = scanner.nextInt();

		// Setar portas automaticamente
		Servidor server = new Servidor(ip, port, ipLeader, portLeader);
		server.listen(port);
	}

}
