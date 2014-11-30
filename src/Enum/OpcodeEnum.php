<?php
namespace evseevnn\Cassandra\Enum;

class OpcodeEnum {
	const ERROR = 0x00;
	const STARTUP = 0x01;
	const READY = 0x02;
	const AUTHENTICATE = 0x03;
	const OPTIONS = 0x05;
	const SUPPORTED = 0x06;
	const QUERY = 0x07;
	const RESULT = 0x08;
	const PREPARE = 0x09;
	const EXECUTE = 0x0A;
	const REGISTER = 0x0B;
	const EVENT = 0x0C;
	const AUTH_CHALLENGE = 0x0E;
	const AUTH_RESPONSE = 0x0F;
	const AUTH_SUCCESS = 0x10;
}
