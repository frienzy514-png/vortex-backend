import { Module, forwardRef } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { IntentsService } from "./intents.service";
import { IntentsController } from "./intents.controller";
import { IntentsGateway } from "./intents.gateway";
import { IntentsSweeperService } from "./intents-sweeper.service";
import { IntentsMaintenanceJobs } from "./intents-maintenance.jobs";
import { INTENTS_REPOSITORY, InMemoryIntentsRepository } from "./intents.repository";
import { PrismaIntentsRepository } from "./prisma-intents.repository";
import { IntentCapabilityIndex } from "./solver-intent-matcher";
import { backplaneProvider } from "./backplane/backplane.factory";
import { backplaneHealthIndicator } from "./backplane/backplane-health.provider";
import { SolversModule } from "../solvers/solvers.module";
import { RoutingModule } from "../routing/routing.module";
import { TokensModule } from "../tokens/tokens.module";
import { SorobanModule } from "../soroban/soroban.module";
import { AppConfig } from "../config/configuration";
import { PrismaService } from "../prisma/prisma.service";
import { GovernanceModule } from "../governance/governance.module";
import {
  INTENTS_UNIT_OF_WORK,
  InMemoryIntentsUnitOfWork,
  PrismaIntentsUnitOfWork,
} from "./intents.unit-of-work";
import { IIntentsRepository } from "./intents.repository";
import { InMemoryOutboxRepository, OUTBOX_REPOSITORY } from "../soroban/outbox.repository";
import { PrismaOutboxRepository } from "../soroban/prisma-outbox.repository";
import { OutboxRelayService } from "../soroban/outbox-relay.service";
import {
  InMemoryPendingSlashesRepository,
  PENDING_SLASHES_REPOSITORY,
} from "../solvers/pending-slashes.repository";
import { PrismaPendingSlashesRepository } from "../solvers/prisma-pending-slashes.repository";
import { SlashingPipelineService } from "./slashing-pipeline.service";
import { AdminSlashesController, SlashesController } from "./slashes.controller";
import { OutboxAdminController } from "../soroban/outbox-admin.controller";

/** The outbox, unit of work, and slash saga share INTENTS_PERSISTENCE with the intents store. */
const usePrisma = () => (process.env.INTENTS_PERSISTENCE ?? "memory") === "prisma";

@Module({
  // Both SolversModule and SorobanModule import IntentsModule back, so both
  // edges of each cycle must be deferred — a bare import resolves to `undefined`
  // when the peer module is still mid-initialization (AppModule reaches
  // SorobanModule through HealthModule before IntentsModule has finished).
  // `forwardRef` on the SorobanModule import mirrors the one in SorobanModule:
  // the two modules need each other (ShadowService here, IntentsService there).
  imports: [
    forwardRef(() => SolversModule),
    RoutingModule,
    TokensModule,
    forwardRef(() => SorobanModule),
  ],
  imports: [forwardRef(() => SolversModule), RoutingModule, TokensModule, SorobanModule, GovernanceModule],
  controllers: [IntentsController, SlashesController, AdminSlashesController, OutboxAdminController],
  providers: [
    // Select the persistence adapter based on INTENTS_PERSISTENCE env var.
    // INTENTS_PERSISTENCE=prisma  → PrismaIntentsRepository (production/staging)
    // INTENTS_PERSISTENCE=memory  → InMemoryIntentsRepository (default, dev/test)
    {
      provide: INTENTS_REPOSITORY,
      inject: [ConfigService, PrismaService],
      useFactory: (config: ConfigService<AppConfig, true>, prisma: PrismaService) => {
        const adapter = process.env.INTENTS_PERSISTENCE ?? "memory";
        if (adapter === "prisma") {
          return new PrismaIntentsRepository(prisma);
        }
        return new InMemoryIntentsRepository();
      },
    },
    // Transactional outbox (issue #396).
    {
      provide: OUTBOX_REPOSITORY,
      inject: [PrismaService],
      useFactory: (prisma: PrismaService) =>
        usePrisma() ? new PrismaOutboxRepository(prisma) : new InMemoryOutboxRepository(),
    },
    {
      provide: INTENTS_UNIT_OF_WORK,
      inject: [PrismaService, INTENTS_REPOSITORY, OUTBOX_REPOSITORY],
      useFactory: (
        prisma: PrismaService,
        intents: IIntentsRepository,
        outbox: InMemoryOutboxRepository | PrismaOutboxRepository,
      ) =>
        outbox instanceof InMemoryOutboxRepository
          ? new InMemoryIntentsUnitOfWork(intents, outbox)
          : new PrismaIntentsUnitOfWork(prisma),
    },
    OutboxRelayService,
    // Slashing saga (issue #397).
    {
      provide: PENDING_SLASHES_REPOSITORY,
      inject: [PrismaService],
      useFactory: (prisma: PrismaService) =>
        usePrisma() ? new PrismaPendingSlashesRepository(prisma) : new InMemoryPendingSlashesRepository(),
    },
    SlashingPipelineService,
    IntentsService,
    IntentCapabilityIndex,
    backplaneProvider,
    IntentsGateway,
    backplaneHealthIndicator,
    IntentsSweeperService,
    IntentsMaintenanceJobs,
    // Note: EventIngestionService is provided by SorobanModule (imported above)
    // and exported from there — no re-declaration needed here.
  ],
  exports: [IntentsService, IntentsGateway, IntentCapabilityIndex],
})
export class IntentsModule {}
