package com.tech.config

import io.github.cdimascio.dotenv.Dotenv

object AppConfig {
  private val dotenv = Dotenv.configure()
    .ignoreIfMissing()
    .load()

  def get(key: String, default: String = ""): String =
    Option(dotenv.get(key)).orElse(sys.env.get(key)).getOrElse(default)

  val accessKey: String = get("FLOCI_ACCESS_KEY", "test")
  val secretKey: String = get("FLOCI_SECRET_KEY", "test")
  val endpoint:  String = get("FLOCI_ENDPOINT", "http://localhost:4566")
}