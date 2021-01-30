import sbt._
import Keys._
import sbt.io.Path
import sbt.librarymanagement.{MavenRepository, Resolver}
import sbt.librarymanagement.ivy.Credentials

object inbaytech {

  lazy val publishToRepository = {
    val props = sys.props
    props.get("sbt.publish.host").orElse(sys.env.get("SBT_PUBLISH_HOST")).flatMap { nexus =>
      props.get("sbt.publish.repo").orElse(sys.env.get("SBT_PUBLISH_REPO")).map { repository =>
        s"$repository" at s"https://$nexus/repository/$repository"
      }
    }
  }

  def ivyCredentials(resolver: Option[Resolver]): Seq[Credentials] = {
    resolver.map {
      case maven: MavenRepository =>
        Seq[Credentials](Credentials(Path.userHome / ".ivy2" / "credentials" / uri(maven.root).getHost))
    }.getOrElse(Seq[Credentials]())
  }

  lazy val settings = Def.settings(
    organization := "com.inbaytech",
    publishTo := inbaytech.publishToRepository,
    credentials ++= inbaytech.ivyCredentials(publishTo.value)
  )
}

