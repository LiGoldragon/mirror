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
          schemaFilter =
            path: type:
            (type == "regular" || type == "directory") && (builtins.match ".*/schema(/.*)?" path != null);
          sourceFilter = path: type: (craneLib.filterCargoSources path type) || (schemaFilter path type);
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter = sourceFilter;
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
          # The two-VM criome-auth witness build: the daemon + nota-text CLIs
          # PLUS the mirror-landed-body-verifier (the `witness` feature also
          # enables nota-text). node-b installs this so it can re-hash the
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
          test-nota-text = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--features nota-text --all-targets";
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
              cargoTestExtraArgs = "--test landed_body_readback restore_hands_back_the_landed_genesis_body_which_rehashes_to_the_head -- --exact";
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
