//
//  ZenPostgres.swift
//  ZenPostgres
//
//  Created by Gerardo Grisolini on 17/03/2019.
//

import Foundation
import PostgresKit
import Logging


public protocol ZenPostgresProtocol {
    func connect() -> EventLoopFuture<PostgresConnection>
    func disconnect(_ connection: PostgresConnection)
    func close() throws
    func log(_ message: String)
}

public class ZenPostgres: ZenPostgresProtocol {
    public static var pool: ZenPostgres!

    private let logger: Logger
    private let eventLoopGroup: EventLoopGroup
    private let connectionPool: ConnectionPool<PostgresConnectionSource>
       
    public init(config: PostgresConfig, eventLoopGroup: EventLoopGroup) {
        self.logger = config.logger
        self.eventLoopGroup = eventLoopGroup
        
        let configuration = PostgresConfiguration(
            hostname: config.host,
            port: config.port,
            username: config.username,
            password: config.password,
            database: config.database,
            tlsConfiguration: config.tls ? TLSConfiguration.forClient(certificateVerification: .none) : nil
        )
        
        let source = PostgresConnectionSource(configuration: configuration)
        
        connectionPool = ConnectionPool(configuration: .init(maxConnections: config.maximumConnections), source: source, logger: config.logger, on: self.eventLoopGroup)
        
        ZenPostgres.pool = self
        
        logger.info(Logger.Message(stringLiteral: "▶️ ZenPostgres started on \(config.host):\(config.port)"))
    }

    public func newPromise<T>() -> EventLoopPromise<T> {
        return eventLoopGroup.next().makePromise(of: T.self)
    }

    public func connect() -> EventLoopFuture<PostgresConnection> {
        return connectionPool.requestConnection().map { conn -> PostgresConnection in
            self.log("▶️ Connect")
            return conn
        }
    }

    public func disconnect(_ connection: PostgresConnection) {
        self.log("⏹️ Disconnect")
        connectionPool.releaseConnection(connection)
    }

    public func close() throws {
        self.log("⏹️ Stopped ZenPostgres")
        connectionPool.shutdown()
        try eventLoopGroup.syncShutdownGracefully()
    }
    
    public func log(_ message: String) {
        logger.debug(Logger.Message(stringLiteral: message))
    }
}

extension PostgresConnection {
    public func disconnect() {
        ZenPostgres.pool.disconnect(self)
    }
}

extension String {
    func capitalizingFirstLetter() -> String {
        return prefix(1).capitalized + dropFirst()
    }
    
    mutating func capitalizeFirstLetter() {
        self = self.capitalizingFirstLetter()
    }
}

public enum ZenError: Error {
    case connectionNotFound
    case recordNotFound
    case recordNotSave
    case passwordDoesNotMatch
    case error(_ message: String)
}

public struct DataSourceJoin {
    public let table:            String
    public let direction:        JoinType
    public let onCondition:      String
    
    public init(table: String, onCondition: String = "", direction: JoinType = .INNER) {
        self.table = table
        self.direction = direction
        self.onCondition = onCondition
    }
}

public enum JoinType {
    case INNER
    case OUTER
    case LEFT
    case RIGHT
}

public struct Cursor {
    public var limit:           Int = 50
    public var offset:          Int = 0
    public var totalRecords:    Int = 0
    public init() {}
    
    public init(limit: Int, offset: Int) {
        self.limit      = limit
        self.offset     = offset
    }
    
    public init(limit: Int, offset: Int, totalRecords: Int) {
        self.limit          = limit
        self.offset         = offset
        self.totalRecords   = totalRecords
    }
}
