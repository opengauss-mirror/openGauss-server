package gauss.regress.jdbc.utils;

public class SQLCommand {
	String command;
	CommandType commandType;
	public SQLCommand(String command, CommandType commandType) {
		super();
		this.command = command;
		this.commandType = commandType;
	}
	public String getCommand() {
		return command;
	}
	public void setCommand(String command) {
		this.command = command;
	}
	public CommandType getCommandType() {
		return commandType;
	}
	public void setCommandType(CommandType commandType) {
		this.commandType = commandType;
	}
	
}
