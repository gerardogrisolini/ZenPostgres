//
//  PostgresConfig.swift
//  ZenPostgres
//
//  Created by Gerardo Grisolini on 17/03/2019.
//

public struct PostgresConfig {
    public let host: String
    public let port: Int
    public let tls: Bool
    public let username: String
    public let password: String
    public let database: String
    public let maximumConnections: Int
    
    public init(host: String,
                port: Int,
                tls: Bool,
                username: String,
                password: String,
                database: String,
                maximumConnections: Int = 10) {
        self.host = host
        self.port = port
        self.tls = tls
        self.username = username
        self.password = password
        self.database = database
        self.maximumConnections = maximumConnections
    }
}
