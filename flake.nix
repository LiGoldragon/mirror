{
  description = "mirror - Payload-blind append-ingest mirror daemon for sema version control.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";

    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";

    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      crane,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forSystems = function: nixpkgs.lib.genAttrs systems (system: function system);

      mkContext =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          toolchain = fenix.packages.${system}.complete.withComponents [
            "cargo"
            "rustc"
            "rustfmt"
            "clippy"
            "rust-analyzer"
            "rust-src"
          ];
          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter = craneLib.filterCargoSources;
            name = "source";
          };
          commonArgs = {
            inherit src;
            strictDeps = true;
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        in
        {
          inherit
            pkgs
            toolchain
            craneLib
            src
            commonArgs
            cargoArtifacts
            ;
        };
    in
    {
      packages = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.buildPackage (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              pname = "mirror";
              meta.mainProgram = "mirror-daemon";
            }
          );
          # The two-VM criome-auth witness build: the daemon + Dotos CLIs
          # PLUS the mirror-landed-body-verifier (the `witness` feature also
          # enables dotos-text). node-b installs this so it can re-hash the
          # landed body in the VM. Consumed by CriomOS-test-cluster.
          witness = context.craneLib.buildPackage (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoExtraArgs = "--features witness";
              pname = "mirror-witness";
              meta.mainProgram = "mirror-daemon";
            }
          );
        }
      );

      checks = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
            }
          );
          build = context.craneLib.cargoBuild (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
            }
          );
          test = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
            }
          );
          test-dotos-text = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--features dotos-text --all-targets";
            }
          );
          # A REAL landed body is read back OUT of the mirror over the existing
          # working contract (a zero-coverage PublishCheckpoint then Restore), and
          # re-deriving its content address reproduces the head — the two-VM
          # witness's wire readback, with no in-process handle and no new wire op.
          mirror-restore-hands-back-landed-body = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test landed_body_readback restore_returns_the_exact_landed_body_and_its_content_address -- --exact";
            }
          );
          # A SemaVersionedLog store recomputes each body's content address at
          # append and REFUSES a digest-mismatched body before it lands
          # (landed_entries empty, head None), the faithful body lands and
          # re-hashes to the head, and an Opaque control lands the same tampered
          # body unchanged — the append-time twin of the post-landing verifier.
          mirror-append-refuses-digest-mismatch = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test append_addressing_refusal addressing_policy_refuses_tampering_without_weakening_opaque_stores -- --exact";
            }
          );
          # The in-VM witness verifier bin compiles under `--features witness`
          # and its digest-hex decode round-trips. The bin's re-hash itself is
          # the SAME `LandedBody::content_address` proven by the check above.
          mirror-landed-body-verifier-builds = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--features witness --bin mirror-landed-body-verifier";
            }
          );
          fmt = context.craneLib.cargoFmt {
            inherit (context) src;
          };
          clippy = context.craneLib.cargoClippy (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets -- -D warnings";
            }
          );
        }
      );

      apps = forSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/mirror";
        };
        daemon = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/mirror-daemon";
        };
        meta = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/meta-mirror";
        };
        write-configuration = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/mirror-write-configuration";
        };
      });

      formatter = forSystems (system: (mkContext system).pkgs.nixfmt-rfc-style);

      devShells = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.pkgs.mkShell {
            name = "mirror";
            packages = [
              context.pkgs.jujutsu
              context.toolchain
            ];
          };
        }
      );
    };
}
