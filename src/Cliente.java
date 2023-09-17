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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;

public class Cliente {
	private InetAddress ip;
	private int port;
	private Long timestamp;
	// Estruturas de dados para armazenar o hash dos valores e timestamps
	private Map<String, String> hashVal;
	private Map<String, Long> hashTime;
	private static List<ServerInfo> servers; // Lista de servidores acessíveis

	public Cliente(InetAddress ipS, int portS, List<ServerInfo> servers) {
		setIp(ipS);
		setPort(portS);
		this.hashTime = new HashMap<>();
		this.timestamp = (new Timestamp(System.currentTimeMillis())).getTime(); // Tomar o tempo de instanciação
		hashVal = new HashMap<>();
		hashTime = new HashMap<>();
		Cliente.setServers(servers);
	}

	public void setIp(InetAddress ip) {
		this.ip = ip;
	}

	public InetAddress getIp() {
		return this.ip;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getPort() {
		return this.port;
	}

	public static void setServers(List<ServerInfo> servers) {
		Cliente.servers = servers;
	}

	public static List<ServerInfo> getServers() {
		return servers;
	}

	public static ServerInfo chooseRandomServer() {
		List<ServerInfo> list = Cliente.getServers();
		Random random = new Random();
		int randomIndex = random.nextInt(list.size());
		return list.get(randomIndex);
	}

	public static void printServers(List<ServerInfo> servers) {
		System.out.println("\nServidores capturados:");
		for (ServerInfo server : servers) {
			System.out.println(server);
		}
	}

	// Método para calcular o timestamp considerando 0 como o momento de
	// instanciação
	private long getTimestamp() {
		return (new Timestamp(System.currentTimeMillis())).getTime() - this.timestamp;
	}

	// Classe para armazenar informações de IP e porta do servidor
	public static class ServerInfo {
		private InetAddress ip;
		private int port;

		public ServerInfo(InetAddress ip, int port) {
			setIp(ip);
			setPort(port);
		}

		public void setIp(InetAddress ip) {
			this.ip = ip;
		}

		public InetAddress getIp() {
			return ip;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public int getPort() {
			return port;
		}

		@Override
		public String toString() {
			return "IP: " + ip + ", Porta: " + port;
		}
	}

	// Método para capturar os dados de IP e porta dos servidores
	public static Cliente inputCapt() throws UnknownHostException {
		List<ServerInfo> servers = new ArrayList<>();
		Scanner scanner = new Scanner(System.in);
		System.out.println("Insira seu IP:");
		InetAddress ipS = InetAddress.getByName(scanner.next());
		System.out.println("Insira sua porta:");
		int portS = scanner.nextInt();

		for (int i = 1; i <= 3; i++) {
			System.out.println("Insira o IP do servidor " + i + ":");
			InetAddress ip = InetAddress.getByName(scanner.next());
			System.out.println("Insira a porta do servidor " + i + ":");
			int port = scanner.nextInt();
			servers.add(new ServerInfo(ip, port));
		}
		Cliente client = new Cliente(ipS, portS, servers);
		return client;
	}

	// Aceita requisições e atribui uma Thread
	public void listen(int port) throws UnknownHostException, IOException {
		Cliente cli = this;

		new Thread() {
			private ServerSocket listenSocket;

			public void run() {
				try {
					listenSocket = new ServerSocket(port);
					while (true) {
						Socket connectSocket = listenSocket.accept();
						TCPConnResThread requestThread = new TCPConnResThread(connectSocket, cli);
						requestThread.start();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}.start();
	}

	static class TCPConnResThread extends Thread {
		private Socket soc;
		private Cliente cli;

		public TCPConnResThread(Socket soc, Cliente cli) {
			this.soc = soc;
			this.cli = cli;
		}

		@Override
		public void run() {
			try {
				// Leitura de mensagem
				Mensagem msgMessage = getResponse();

				// Tratamento da mensagem recebida
				msgHandling(msgMessage, cli);

				// Closing connection
				soc.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// tomar resposta do servidor
		private Mensagem getResponse() throws IOException {
			InputStreamReader istream = new InputStreamReader(soc.getInputStream());
			BufferedReader reader = new BufferedReader(istream);
			String msgRes = reader.readLine();
			Mensagem msgMessage = new Gson().fromJson(msgRes, Mensagem.class);
			return msgMessage;
		}

		private void msgHandling(Mensagem msgRes, Cliente client) {
			String msgType = msgRes.getType();
			if (msgType.equals("PUT_OK")) {
				// Mensagem PUT_OK recebida
				System.out.println("PUT_OK key: " + msgRes.getKey() + " value " + msgRes.getValue() + " timestamp "
						+ msgRes.getTimestamp() + " realizada no servidor " + msgRes.getIpS().getHostAddress() + ":"
						+ msgRes.getPortS());

				// Atualizar a tabela local com o valor recebido
				client.hashVal.put(msgRes.getKey(), msgRes.getKey());
				client.hashTime.put(msgRes.getKey(), msgRes.getTimestamp());
			} else if (msgType.equals("GET")) {
				// Mensagem GET_OK recebida
				Long hashTs;
				if (client.hashTime.get(msgRes.getKey()) == null)
					hashTs = (long) 0;
				else
					hashTs = client.hashTime.get(msgRes.getKey());

				System.out.println("GET key: " + msgRes.getKey() + " value: " + msgRes.getValue()
						+ " obtido do servidor " + msgRes.getIpS().getHostAddress() + ":" + msgRes.getPortS()
						+ " meu timestamp " + hashTs + " e do servidor " + msgRes.getTimestamp());

				// Atualizar a tabela local com o valor recebido
				if (!msgRes.getValue().equals("TRY_OTHER_SERVER_OR_LATER")) {
					client.hashVal.put(msgRes.getKey(), msgRes.getValue());
					client.hashTime.put(msgRes.getKey(), msgRes.getTimestamp());
				}
			}
		}
	}

	private static void sendMessage(Mensagem msg, Cliente client) {
		TCPConnSendThread TcpClient = new TCPConnSendThread(msg, client);
		TcpClient.start();
	}

	// Classe aninhada representando uma thread para para enviar e receber
	// mensagens.
	static class TCPConnSendThread extends Thread {
		private Mensagem msg;

		public TCPConnSendThread(Mensagem msg, Cliente client) {
			this.msg = msg;
		}

		@Override
		public void run() {
			try {
				// Envio de requisição GET ou PUT para os servidores
				sendMessage();
			} catch (IOException e) {
			}

		}

		private void sendMessage() throws IOException {
			// Socket para conexão
			Socket clientSoc = new Socket(this.msg.getIpD(), this.msg.getPortD());

			// Envio de mensagem
			OutputStream ostream = clientSoc.getOutputStream();
			DataOutputStream writer = new DataOutputStream(ostream);
			String msgJson = new Gson().toJson(msg);
			writer.writeBytes(msgJson + "\n");
			clientSoc.close();
			writer.close();
		}
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		// Captura IP e portas dos Servers
		// Instânciação do Cliente
		Cliente client = inputCapt();

		// Menu Interativo
		Scanner scanner = new Scanner(System.in);
		client.listen(client.getPort());
		while (true) {
			TimeUnit.SECONDS.sleep(1);
			System.out.print("\nPUT(1)\nGET(2)\n>>>");
			try {
				int option = scanner.nextInt();
				switch (option) {
				case 1:
					// Opção PUT
					System.out.print("\nKey\n>>>");
					String putKey = scanner.next();
					System.out.print("\nValue\n>>>");
					String putValue = scanner.next();

					ServerInfo randomServer = chooseRandomServer();
					Mensagem putMsg = new Mensagem(putKey, putValue, "PUT", client.getTimestamp(), client.ip,
							client.port, randomServer.getIp(), randomServer.getPort());
					sendMessage(putMsg, client);
					break;
				case 2:
					// Opção GET
					System.out.print("\nKey\n>>>");
					String getKey = scanner.next();

					ServerInfo randomServer1 = chooseRandomServer();
					Long hashTs;
					if (client.hashTime.get(getKey) == null)
						hashTs = (long) 0;
					else
						hashTs = client.hashTime.get(getKey);

					Mensagem getMsg = new Mensagem(getKey, "NULL", "GET", hashTs, client.ip, client.port,
							randomServer1.getIp(), randomServer1.getPort());
					sendMessage(getMsg, client);
					break;
				default:
					System.out.print("Opção inválida\n");
				}
			} catch (NumberFormatException | InputMismatchException e) {
				System.out.print("Opção inválida\n");
				scanner.next();

			}
		}
	}

}